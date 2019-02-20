#include <Storages/MergeTree/MergeTreeBloomFilterIndex.h>

#include <Common/UTF8Helpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Parsers/ASTLiteral.h>

#include <Poco/Logger.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}


MergeTreeBloomFilterIndexGranule::MergeTreeBloomFilterIndexGranule(const MergeTreeBloomFilterIndex & index)
    : IMergeTreeIndexGranule()
    , index(index)
    , bloom_filter(index.bloom_filter_size, index.bloom_filter_hashes, index.seed)
    , has_elems(false)
{
}

void MergeTreeBloomFilterIndexGranule::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(
                "Attempt to write empty minmax index `" + index.name + "`", ErrorCodes::LOGICAL_ERROR);

    const auto & filter = bloom_filter.getFilter();
    ostr.write(reinterpret_cast<const char *>(filter.data()), index.bloom_filter_size);
}

void MergeTreeBloomFilterIndexGranule::deserializeBinary(ReadBuffer & istr)
{
    std::vector<UInt8> filter(index.bloom_filter_size, 0);
    istr.read(reinterpret_cast<char *>(filter.data()), index.bloom_filter_size);
    bloom_filter.setFilter(std::move(filter));
    has_elems = true;
}

void MergeTreeBloomFilterIndexGranule::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    size_t rows_read = std::min(limit, block.rows() - *pos);

    const auto & column = block.getByName(index.columns.front()).column;
    for (size_t i = 0; i < rows_read; ++i)
    {
        auto ref = column->getDataAt(*pos + i);
        size_t cur = 0;
        size_t token_start = 0;
        size_t token_len = 0;

        while (cur < ref.size && index.tokenExtractorFunc(ref.data, ref.size, &cur, &token_start, &token_len))
            bloom_filter.add(ref.data + token_start, token_len);
    }

    has_elems = true;
    *pos += rows_read;
}


MergeTreeIndexGranulePtr MergeTreeBloomFilterIndex::createIndexGranule() const
{
    return std::make_shared<MergeTreeBloomFilterIndexGranule>(*this);
}

IndexConditionPtr MergeTreeBloomFilterIndex::createIndexCondition(
        const SelectQueryInfo & query, const Context & context) const
{
    return std::make_shared<BloomFilterCondition>(query, context, *this);
};


struct NgramTokenExtractor
{
    NgramTokenExtractor(size_t n) : n(n) {}

    static String getName() {
        static String name = "ngrambf";
        return name;
    }

    bool operator() (const char * data, size_t len, size_t * pos, size_t * token_start, size_t * token_len)
    {
        *token_start = *pos;
        *token_len = 0;
        for (size_t i = 0; i < n; ++i)
        {
            size_t sz = UTF8::seqLength(static_cast<UInt8>(data[*token_start + *token_len]));
            if (*token_start + *token_len + sz > len) {
                return false;
            }
            *token_len += sz;
        }
        ++*pos;
        return true;
    }

    size_t n;
};


std::unique_ptr<IMergeTreeIndex> bloomFilterIndexCreator(
        const NamesAndTypesList & new_columns,
        std::shared_ptr<ASTIndexDeclaration> node,
        const MergeTreeData & data,
        const Context & context)
{
    if (node->name.empty())
        throw Exception("Index must have unique name", ErrorCodes::INCORRECT_QUERY);

    ASTPtr expr_list = MergeTreeData::extractKeyExpressionList(node->expr->clone());

    /// TODO: support a number of columns.
    if (expr_list->children.size() > 1)
        throw Exception(node->name + " index can be used only with one column.", ErrorCodes::INCORRECT_QUERY);

    auto syntax = SyntaxAnalyzer(context, {}).analyze(
            expr_list, new_columns);
    auto index_expr = ExpressionAnalyzer(expr_list, syntax, context).getActions(false);

    auto sample = ExpressionAnalyzer(expr_list, syntax, context)
            .getActions(true)->getSampleBlock();

    Names columns;
    DataTypes data_types;

    for (size_t i = 0; i < expr_list->children.size(); ++i)
    {
        const auto & column = sample.getByPosition(i);

        columns.emplace_back(column.name);
        data_types.emplace_back(column.type);

        if (data_types.back()->getTypeId() != TypeIndex::String
            && data_types.back()->getTypeId() != TypeIndex::FixedString)
            throw Exception(node->name + " index can be used only with `String` and `FixedString` column.", ErrorCodes::INCORRECT_QUERY);
    }

    if (node->name == NgramTokenExtractor::getName()) {
        if (!node->type->arguments || node->type->arguments->children.size() != 3)
            throw Exception(node->name + " index must have exactly 3 arguments.", ErrorCodes::INCORRECT_QUERY);

        size_t n = typeid_cast<const ASTLiteral &>(
                *node->type->arguments->children[0]).value.get<size_t>();
        size_t bloom_filter_size = typeid_cast<const ASTLiteral &>(
                *node->type->arguments->children[1]).value.get<size_t>();
        size_t seed = typeid_cast<const ASTLiteral &>(
                *node->type->arguments->children[2]).value.get<size_t>();\

        auto bloom_filter_hashes = static_cast<size_t>(
                n * log(2.) / (node->granularity * data.index_granularity));

        return std::make_unique<MergeTreeBloomFilterIndex>(
                node->name, std::move(index_expr), columns, data_types, sample, node->granularity,
                bloom_filter_size, bloom_filter_hashes, seed, NgramTokenExtractor(n));
    } else {
        throw Exception("Unknown index type: `" + node->name + "`.", ErrorCodes::LOGICAL_ERROR);
    }
}

}
