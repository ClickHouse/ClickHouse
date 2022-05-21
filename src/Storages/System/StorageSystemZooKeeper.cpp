#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Storages/System/StorageSystemZooKeeper.h>
#include <Storages/SelectQueryInfo.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTSubquery.h>
#include <Interpreters/Set.h>
#include <Interpreters/interpretSubquery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <algorithm>
#include <deque>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


NamesAndTypesList StorageSystemZooKeeper::getNamesAndTypes()
{
    return {
        { "name",           std::make_shared<DataTypeString>() },
        { "value",          std::make_shared<DataTypeString>() },
        { "czxid",          std::make_shared<DataTypeInt64>() },
        { "mzxid",          std::make_shared<DataTypeInt64>() },
        { "ctime",          std::make_shared<DataTypeDateTime>() },
        { "mtime",          std::make_shared<DataTypeDateTime>() },
        { "version",        std::make_shared<DataTypeInt32>() },
        { "cversion",       std::make_shared<DataTypeInt32>() },
        { "aversion",       std::make_shared<DataTypeInt32>() },
        { "ephemeralOwner", std::make_shared<DataTypeInt64>() },
        { "dataLength",     std::make_shared<DataTypeInt32>() },
        { "numChildren",    std::make_shared<DataTypeInt32>() },
        { "pzxid",          std::make_shared<DataTypeInt64>() },
        { "path",           std::make_shared<DataTypeString>() },
    };
}

/// Type of path to be fetched
enum class ZkPathType
{
    Exact,   /// Fetch all nodes under this path
    Prefix,  /// Fetch all nodes starting with this prefix, recursively (multiple paths may match prefix)
    Recurse, /// Fatch all nodes under this path, recursively
};

/// List of paths to be feched from zookeeper
using Paths = std::deque<std::pair<String, ZkPathType>>;

static String pathCorrected(const String & path)
{
    String path_corrected;
    /// path should starts with '/', otherwise ZBADARGUMENTS will be thrown in
    /// ZooKeeper::sendThread and the session will fail.
    if (path.empty() || path[0] != '/')
        path_corrected = '/';
    path_corrected += path;
    /// In all cases except the root, path must not end with a slash.
    if (path_corrected != "/" && path_corrected.back() == '/')
        path_corrected.resize(path_corrected.size() - 1);
    return path_corrected;
}


static bool extractPathImpl(const IAST & elem, Paths & res, ContextPtr context, bool allow_unrestricted)
{
    const auto * function = elem.as<ASTFunction>();
    if (!function)
        return false;

    if (function->name == "and")
    {
        for (const auto & child : function->arguments->children)
            if (extractPathImpl(*child, res, context, allow_unrestricted))
                return true;

        return false;
    }

    const auto & args = function->arguments->as<ASTExpressionList &>();
    if (args.children.size() != 2)
        return false;

    if (function->name == "in")
    {
        const ASTIdentifier * ident = args.children.at(0)->as<ASTIdentifier>();
        if (!ident || ident->name() != "path")
            return false;

        ASTPtr value = args.children.at(1);

        if (value->as<ASTSubquery>())
        {
            auto interpreter_subquery = interpretSubquery(value, context, {}, {});
            auto pipeline = interpreter_subquery->execute().pipeline;
            SizeLimits limites(context->getSettingsRef().max_rows_in_set, context->getSettingsRef().max_bytes_in_set, OverflowMode::THROW);
            Set set(limites, true, context->getSettingsRef().transform_null_in);
            set.setHeader(pipeline.getHeader().getColumnsWithTypeAndName());

            PullingPipelineExecutor executor(pipeline);
            Block block;
            while (executor.pull(block))
            {
                set.insertFromBlock(block.getColumnsWithTypeAndName());
            }
            set.finishInsert();

            set.checkColumnsNumber(1);
            const auto & set_column = *set.getSetElements()[0];
            for (size_t row = 0; row < set_column.size(); ++row)
                res.emplace_back(set_column[row].safeGet<String>(), ZkPathType::Exact);
        }
        else
        {
            auto evaluated = evaluateConstantExpressionAsLiteral(value, context);
            const auto * literal = evaluated->as<ASTLiteral>();
            if (!literal)
                return false;

            if (String str; literal->value.tryGet(str))
            {
                res.emplace_back(str, ZkPathType::Exact);
            }
            else if (Tuple tuple; literal->value.tryGet(tuple))
            {
                for (auto element : tuple)
                    res.emplace_back(element.safeGet<String>(), ZkPathType::Exact);
            }
            else
                return false;
        }

        return true;
    }
    else if (function->name == "equals")
    {
        const ASTIdentifier * ident;
        ASTPtr value;
        if ((ident = args.children.at(0)->as<ASTIdentifier>()))
            value = args.children.at(1);
        else if ((ident = args.children.at(1)->as<ASTIdentifier>()))
            value = args.children.at(0);
        else
            return false;

        if (ident->name() != "path")
            return false;

        auto evaluated = evaluateConstantExpressionAsLiteral(value, context);
        const auto * literal = evaluated->as<ASTLiteral>();
        if (!literal)
            return false;

        if (literal->value.getType() != Field::Types::String)
            return false;

        res.emplace_back(literal->value.safeGet<String>(), ZkPathType::Exact);
        return true;
    }
    else if (allow_unrestricted && function->name == "like")
    {
        const ASTIdentifier * ident;
        ASTPtr value;
        if ((ident = args.children.at(0)->as<ASTIdentifier>()))
            value = args.children.at(1);
        else if ((ident = args.children.at(1)->as<ASTIdentifier>()))
            value = args.children.at(0);
        else
            return false;

        if (ident->name() != "path")
            return false;

        auto evaluated = evaluateConstantExpressionAsLiteral(value, context);
        const auto * literal = evaluated->as<ASTLiteral>();
        if (!literal)
            return false;

        if (literal->value.getType() != Field::Types::String)
            return false;

        String pattern = literal->value.safeGet<String>();
        bool has_metasymbol = false;
        String prefix; // pattern prefix before the first metasymbol occurrence
        for (size_t i = 0; i < pattern.size(); i++)
        {
            char c = pattern[i];
            // Handle escaping of metasymbols
            if (c == '\\' && i + 1 < pattern.size())
            {
                char c2 = pattern[i + 1];
                if (c2 == '_' || c2 == '%')
                {
                    prefix.append(1, c2);
                    i++; // to skip two bytes
                    continue;
                }
            }

            // Stop prefix on the first metasymbols occurrence
            if (c == '_' || c == '%')
            {
                has_metasymbol = true;
                break;
            }

            prefix.append(1, c);
        }

        res.emplace_back(prefix, has_metasymbol ? ZkPathType::Prefix : ZkPathType::Exact);

        return true;
    }

    return false;
}


/** Retrieve from the query a condition of the form `path = 'path'`, from conjunctions in the WHERE clause.
  */
static Paths extractPath(const ASTPtr & query, ContextPtr context, bool allow_unrestricted)
{
    const auto & select = query->as<ASTSelectQuery &>();
    if (!select.where())
        return allow_unrestricted ? Paths{{"/", ZkPathType::Recurse}} : Paths();

    Paths res;
    return extractPathImpl(*select.where(), res, context, allow_unrestricted) ? res : Paths();
}


void StorageSystemZooKeeper::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    Paths paths = extractPath(query_info.query, context, context->getSettingsRef().allow_unrestricted_reads_from_keeper);

    zkutil::ZooKeeperPtr zookeeper = context->getZooKeeper();

    if (paths.empty())
        throw Exception("SELECT from system.zookeeper table must contain condition like path = 'path' or path IN ('path1','path2'...) or path IN (subquery) in WHERE clause unless `set allow_unrestricted_reads_from_keeper = 'true'`.", ErrorCodes::BAD_ARGUMENTS);

    std::unordered_set<String> added;
    while (!paths.empty())
    {
        auto [path, path_type] = std::move(paths.front());
        paths.pop_front();

        String prefix;
        if (path_type == ZkPathType::Prefix)
        {
            prefix = path;
            size_t last_slash = prefix.rfind('/');
            path = prefix.substr(0, last_slash == String::npos ? 0 : last_slash);
        }

        String path_corrected = pathCorrected(path);

        /// Node can be deleted concurrently. It's Ok, we don't provide any
        /// consistency guarantees for system.zookeeper table.
        zkutil::Strings nodes;
        zookeeper->tryGetChildren(path_corrected, nodes);

        String path_part = path_corrected;
        if (path_part == "/")
            path_part.clear();

        if (!prefix.empty())
        {
            // Remove nodes that do not match specified prefix
            std::erase_if(nodes, [&prefix, &path_part] (const String & node)
            {
                return (path_part + '/' + node).substr(0, prefix.size()) != prefix;
            });
        }

        std::vector<std::future<Coordination::GetResponse>> futures;
        futures.reserve(nodes.size());
        for (const String & node : nodes)
            futures.push_back(zookeeper->asyncTryGet(path_part + '/' + node));

        for (size_t i = 0, size = nodes.size(); i < size; ++i)
        {
            auto res = futures[i].get();
            if (res.error == Coordination::Error::ZNONODE)
                continue; /// Node was deleted meanwhile.

            // Deduplication
            String key = path_part + '/' + nodes[i];
            if (auto [it, inserted] = added.emplace(key); !inserted)
                continue;

            const Coordination::Stat & stat = res.stat;

            size_t col_num = 0;
            res_columns[col_num++]->insert(nodes[i]);
            res_columns[col_num++]->insert(res.data);
            res_columns[col_num++]->insert(stat.czxid);
            res_columns[col_num++]->insert(stat.mzxid);
            res_columns[col_num++]->insert(UInt64(stat.ctime / 1000));
            res_columns[col_num++]->insert(UInt64(stat.mtime / 1000));
            res_columns[col_num++]->insert(stat.version);
            res_columns[col_num++]->insert(stat.cversion);
            res_columns[col_num++]->insert(stat.aversion);
            res_columns[col_num++]->insert(stat.ephemeralOwner);
            res_columns[col_num++]->insert(stat.dataLength);
            res_columns[col_num++]->insert(stat.numChildren);
            res_columns[col_num++]->insert(stat.pzxid);
            res_columns[col_num++]->insert(
                path); /// This is the original path. In order to process the request, condition in WHERE should be triggered.

            if (path_type != ZkPathType::Exact && res.stat.numChildren > 0)
            {
                paths.emplace_back(key, ZkPathType::Recurse);
            }
        }
    }
}


}
