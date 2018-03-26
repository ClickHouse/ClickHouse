#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataStreams/OneBlockInputStream.h>
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


StorageSystemZooKeeper::StorageSystemZooKeeper(const std::string & name_)
    : name(name_)
{
    setColumns(ColumnsDescription({
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
    }));
}


static bool extractPathImpl(const IAST & elem, String & res)
{
    const ASTFunction * function = typeid_cast<const ASTFunction *>(&elem);
    if (!function)
        return false;

    if (function->name == "and")
    {
        for (size_t i = 0; i < function->arguments->children.size(); ++i)
            if (extractPathImpl(*function->arguments->children[i], res))
                return true;

        return false;
    }

    if (function->name == "equals")
    {
        const ASTExpressionList & args = typeid_cast<const ASTExpressionList &>(*function->arguments);
        const IAST * value;

        if (args.children.size() != 2)
            return false;

        const ASTIdentifier * ident;
        if ((ident = typeid_cast<const ASTIdentifier *>(&*args.children.at(0))))
            value = &*args.children.at(1);
        else if ((ident = typeid_cast<const ASTIdentifier *>(&*args.children.at(1))))
            value = &*args.children.at(0);
        else
            return false;

        if (ident->name != "path")
            return false;

        const ASTLiteral * literal = typeid_cast<const ASTLiteral *>(value);
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
    const ASTSelectQuery & select = typeid_cast<const ASTSelectQuery &>(*query);
    if (!select.where_expression)
        return "";

    String res;
    return extractPathImpl(*select.where_expression, res) ? res : "";
}


BlockInputStreams StorageSystemZooKeeper::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    String path = extractPath(query_info.query);
    if (path.empty())
        throw Exception("SELECT from system.zookeeper table must contain condition like path = 'path' in WHERE clause.");

    zkutil::ZooKeeperPtr zookeeper = context.getZooKeeper();

    /// In all cases except the root, path must not end with a slash.
    String path_corrected = path;
    if (path_corrected != "/" && path_corrected.back() == '/')
        path_corrected.resize(path_corrected.size() - 1);

    zkutil::Strings nodes = zookeeper->getChildren(path_corrected);

    String path_part = path_corrected;
    if (path_part == "/")
        path_part.clear();

    std::vector<zkutil::ZooKeeper::TryGetFuture> futures;
    futures.reserve(nodes.size());
    for (const String & node : nodes)
        futures.push_back(zookeeper->asyncTryGet(path_part + '/' + node));

    MutableColumns res_columns = getSampleBlock().cloneEmptyColumns();

    for (size_t i = 0, size = nodes.size(); i < size; ++i)
    {
        auto res = futures[i].get();
        if (!res.exists)
            continue;   /// Node was deleted meanwhile.

        const zkutil::Stat & stat = res.stat;

        size_t col_num = 0;
        res_columns[col_num++]->insert(nodes[i]);
        res_columns[col_num++]->insert(res.value);
        res_columns[col_num++]->insert(Int64(stat.czxid));
        res_columns[col_num++]->insert(Int64(stat.mzxid));
        res_columns[col_num++]->insert(UInt64(stat.ctime / 1000));
        res_columns[col_num++]->insert(UInt64(stat.mtime / 1000));
        res_columns[col_num++]->insert(Int64(stat.version));
        res_columns[col_num++]->insert(Int64(stat.cversion));
        res_columns[col_num++]->insert(Int64(stat.aversion));
        res_columns[col_num++]->insert(Int64(stat.ephemeralOwner));
        res_columns[col_num++]->insert(Int64(stat.dataLength));
        res_columns[col_num++]->insert(Int64(stat.numChildren));
        res_columns[col_num++]->insert(Int64(stat.pzxid));
        res_columns[col_num++]->insert(path);          /// This is the original path. In order to process the request, condition in WHERE should be triggered.
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}


}
