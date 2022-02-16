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

using Paths = Strings;

static String pathCorrected(const String & path)
{
    String path_corrected;
    /// path should starts with '/', otherwise ZBADARGUMENTS will be thrown in
    /// ZooKeeper::sendThread and the session will fail.
    if (path[0] != '/')
        path_corrected = '/';
    path_corrected += path;
    /// In all cases except the root, path must not end with a slash.
    if (path_corrected != "/" && path_corrected.back() == '/')
        path_corrected.resize(path_corrected.size() - 1);
    return path_corrected;
}


static bool extractPathImpl(const IAST & elem, Paths & res, ContextPtr context)
{
    const auto * function = elem.as<ASTFunction>();
    if (!function)
        return false;

    if (function->name == "and")
    {
        for (const auto & child : function->arguments->children)
            if (extractPathImpl(*child, res, context))
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
                res.emplace_back(set_column[row].safeGet<String>());
        }
        else
        {
            auto evaluated = evaluateConstantExpressionAsLiteral(value, context);
            const auto * literal = evaluated->as<ASTLiteral>();
            if (!literal)
                return false;

            if (String str; literal->value.tryGet(str))
            {
                res.emplace_back(str);
            }
            else if (Tuple tuple; literal->value.tryGet(tuple))
            {
                for (auto element : tuple)
                    res.emplace_back(element.safeGet<String>());
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

        res.emplace_back(literal->value.safeGet<String>());
        return true;
    }

    return false;
}


/** Retrieve from the query a condition of the form `path = 'path'`, from conjunctions in the WHERE clause.
  */
static Paths extractPath(const ASTPtr & query, ContextPtr context)
{
    const auto & select = query->as<ASTSelectQuery &>();
    if (!select.where())
        return Paths();

    Paths res;
    return extractPathImpl(*select.where(), res, context) ? res : Paths();
}


void StorageSystemZooKeeper::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    const Paths & paths = extractPath(query_info.query, context);

    zkutil::ZooKeeperPtr zookeeper = context->getZooKeeper();

    std::deque<String> queue;

    bool recursive = true;
    if (paths.empty())
    {
        if (context->getSettingsRef().allow_unrestricted_reads_from_keeper)
            queue.push_back("/");
        else
            throw Exception("SELECT from system.zookeeper table must contain condition like path = 'path' or path IN ('path1','path2'...) or path IN (subquery) in WHERE clause.", ErrorCodes::BAD_ARGUMENTS);
    }
    else
    {
        recursive = false; // do not recurse if path was specified
        for (const auto & path : paths)
        {
            queue.push_back(path);
        }
    }

    std::unordered_set<String> paths_corrected;
    while (!queue.empty())
    {
        const String path = std::move(queue.front());
        queue.pop_front();

        const String & path_corrected = pathCorrected(path);
        auto [it, inserted] = paths_corrected.emplace(path_corrected);
        if (!inserted) /// Do not repeat processing.
            continue;

        zkutil::Strings nodes = zookeeper->getChildren(path_corrected);

        String path_part = path_corrected;
        if (path_part == "/")
            path_part.clear();

        std::vector<std::future<Coordination::GetResponse>> futures;
        futures.reserve(nodes.size());
        for (const String & node : nodes)
            futures.push_back(zookeeper->asyncTryGet(path_part + '/' + node));

        for (size_t i = 0, size = nodes.size(); i < size; ++i)
        {
            auto res = futures[i].get();
            if (res.error == Coordination::Error::ZNONODE)
                continue; /// Node was deleted meanwhile.

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

            if (recursive && res.stat.numChildren > 0)
            {
                queue.push_back(path_part + '/' + nodes[i]);
            }
        }
    }
}


}
