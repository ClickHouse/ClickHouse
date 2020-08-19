#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Storages/System/StorageSystemZooKeeper.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/typeid_cast.h>


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


static bool extractPathImpl(const IAST & elem, String & res)
{
    const auto * function = elem.as<ASTFunction>();
    if (!function)
        return false;

    if (function->name == "and")
    {
        for (const auto & child : function->arguments->children)
            if (extractPathImpl(*child, res))
                return true;

        return false;
    }

    if (function->name == "equals")
    {
        const auto & args = function->arguments->as<ASTExpressionList &>();
        const IAST * value;

        if (args.children.size() != 2)
            return false;

        const ASTIdentifier * ident;
        if ((ident = args.children.at(0)->as<ASTIdentifier>()))
            value = args.children.at(1).get();
        else if ((ident = args.children.at(1)->as<ASTIdentifier>()))
            value = args.children.at(0).get();
        else
            return false;

        if (ident->name != "path")
            return false;

        const auto * literal = value->as<ASTLiteral>();
        if (!literal)
            return false;

        if (literal->value.getType() != Field::Types::String)
            return false;

        res = literal->value.safeGet<String>();
        return true;
    }

    return false;
}


/** Retrieve from the query a condition of the form `path = 'path'`, from conjunctions in the WHERE clause.
  */
static String extractPath(const ASTPtr & query)
{
    const auto & select = query->as<ASTSelectQuery &>();
    if (!select.where())
        return "";

    String res;
    return extractPathImpl(*select.where(), res) ? res : "";
}


void StorageSystemZooKeeper::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const
{
    String path = extractPath(query_info.query);
    if (path.empty())
        throw Exception("SELECT from system.zookeeper table must contain condition like path = 'path' in WHERE clause.", ErrorCodes::BAD_ARGUMENTS);

    zkutil::ZooKeeperPtr zookeeper = context.getZooKeeper();

    String path_corrected;
    /// path should starts with '/', otherwise ZBADARGUMENTS will be thrown in
    /// ZooKeeper::sendThread and the session will fail.
    if (path[0] != '/')
        path_corrected = '/';
    path_corrected += path;
    /// In all cases except the root, path must not end with a slash.
    if (path_corrected != "/" && path_corrected.back() == '/')
        path_corrected.resize(path_corrected.size() - 1);

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
            continue;   /// Node was deleted meanwhile.

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
        res_columns[col_num++]->insert(path);          /// This is the original path. In order to process the request, condition in WHERE should be triggered.
    }
}


}
