#include <Storages/System/StorageSystemMergeTreeMetadataCache.h>

#if USE_ROCKSDB
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeMetadataCache.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

NamesAndTypesList StorageSystemMergeTreeMetadataCache::getNamesAndTypes()
{
    return {
        {"key", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeString>()},
    };
}

static bool extractKeyImpl(const IAST & elem, String & res, bool & precise)
{
    const auto * function = elem.as<ASTFunction>();
    if (!function)
        return false;

    if (function->name == "and")
    {
        for (const auto & child : function->arguments->children)
        {
            bool tmp_precise = false;
            if (extractKeyImpl(*child, res, tmp_precise))
            {
                precise = tmp_precise;
                return true;
            }
        }
        return false;
    }

    if (function->name == "equals" || function->name == "like")
    {
        const auto & args = function->arguments->as<ASTExpressionList &>();
        const IAST * value;

        if (args.children.size() != 2)
            return false;

        const ASTIdentifier * ident;
        if ((ident = args.children.front()->as<ASTIdentifier>()))
            value = args.children.back().get();
        else if ((ident = args.children.back()->as<ASTIdentifier>()))
            value = args.children.front().get();
        else
            return false;

        if (ident->name() != "key")
            return false;

        const auto * literal = value->as<ASTLiteral>();
        if (!literal)
            return false;

        if (literal->value.getType() != Field::Types::String)
            return false;

        res = literal->value.safeGet<String>();
        precise = function->name == "equals";
        return true;
    }
    return false;
}


/// Retrieve from the query a condition of the form `key= 'key'`, from conjunctions in the WHERE clause.
static String extractKey(const ASTPtr & query, bool& precise)
{
    const auto & select = query->as<ASTSelectQuery &>();
    if (!select.where())
        return "";

    String res;
    return extractKeyImpl(*select.where(), res, precise) ? res : "";
}


void StorageSystemMergeTreeMetadataCache::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    bool precise = false;
    String key = extractKey(query_info.query, precise);
    if (key.empty())
        throw Exception(
            "SELECT from system.merge_tree_metadata_cache table must contain condition like key = 'key' or key LIKE 'prefix%' in WHERE clause.", ErrorCodes::BAD_ARGUMENTS);

    auto cache = context->getMergeTreeMetadataCache();
    if (precise)
    {
        String value;
        if (cache->get(key, value) != MergeTreeMetadataCache::Status::OK())
            return;

        size_t col_num = 0;
        res_columns[col_num++]->insert(key);
        res_columns[col_num++]->insert(value);
    }
    else
    {
        String target = extractFixedPrefixFromLikePattern(key);
        if (target.empty())
            throw Exception(
                "SELECT from system.merge_tree_metadata_cache table must contain condition like key = 'key' or key LIKE 'prefix%' in WHERE clause.", ErrorCodes::BAD_ARGUMENTS);

        Strings keys;
        Strings values;
        keys.reserve(4096);
        values.reserve(4096);
        cache->getByPrefix(target, keys, values);
        if (keys.empty())
            return;

        assert(keys.size() == values.size());
        for (size_t i = 0; i < keys.size(); ++i)
        {
            size_t col_num = 0;
            res_columns[col_num++]->insert(keys[i]);
            res_columns[col_num++]->insert(values[i]);
        }
    }
}

}
#endif
