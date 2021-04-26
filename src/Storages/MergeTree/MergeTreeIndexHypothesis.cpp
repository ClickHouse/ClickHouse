#include <Storages/MergeTree/MergeTreeIndexHypothesis.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}


MergeTreeIndexGranuleHypothesis::MergeTreeIndexGranuleHypothesis(const String & index_name_)
    : index_name(index_name_), is_empty(true), met(false)
{
}

MergeTreeIndexGranuleHypothesis::MergeTreeIndexGranuleHypothesis(const String & index_name_, const bool met_)
    : index_name(index_name_), is_empty(false), met(met_)
{
}

void MergeTreeIndexGranuleHypothesis::serializeBinary(WriteBuffer & ostr) const
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt8>());
    size_type->serializeBinary(static_cast<UInt8>(met), ostr);
}

void MergeTreeIndexGranuleHypothesis::deserializeBinary(ReadBuffer & istr)
{
    Field field_met;
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt8>());
    size_type->deserializeBinary(field_met, istr);
    met = field_met.get<UInt8>();
    is_empty = false;
}

MergeTreeIndexAggregatorHypothesis::MergeTreeIndexAggregatorHypothesis(const String & index_name_, const String & column_name_)
    : index_name(index_name_), column_name(column_name_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorHypothesis::getGranuleAndReset()
{
    const auto granule = std::make_shared<MergeTreeIndexGranuleHypothesis>(index_name, met);
    met = true;
    is_empty = true;
    return granule;
}

void MergeTreeIndexAggregatorHypothesis::update(const Block & block, size_t * pos, size_t limit)
{
    size_t rows_read = std::min(limit, block.rows() - *pos);
    if (rows_read == 0)
        return;
    const auto & column = block.getByName(column_name).column->cut(*pos, rows_read);

    if (!column->hasEqualValues() || column->get64(0) == 0)
        met = false;

    is_empty = false;
    *pos += rows_read;
}

MergeTreeIndexConditionHypothesis::MergeTreeIndexConditionHypothesis(
    const String & index_name_,
    const String & column_name_,
    const SelectQueryInfo & query_,
    const Context &)
    : index_name(index_name_)
    , column_name(column_name_)
{
    const auto & select = query_.query->as<ASTSelectQuery &>();

    if (select.where() && select.prewhere())
        expression_ast = makeASTFunction(
            "and",
            select.where()->clone(),
            select.prewhere()->clone());
    else if (select.where())
        expression_ast = select.where()->clone();
    else if (select.prewhere())
        expression_ast = select.prewhere()->clone();
}

std::pair<bool, bool> MergeTreeIndexConditionHypothesis::mayBeTrue(const ASTPtr & ast, const bool value) const
{
    if (ast->getColumnName() == column_name)
        return {value, !value};

    auto * func = ast->as<ASTFunction>();
    if (!func)
        return {true, true};
    auto & args = func->arguments->children;
    if (func->name == "not")
    {
        const auto res = mayBeTrue(args[0], value);
        return {res.second, res.first};
    }
    /*else if (func->name == "or")
    {

    }
    else if (func->name == "and")
    {

    }*/
    else
    {
        return {true, true};
    }
}

bool MergeTreeIndexConditionHypothesis::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    if (idx_granule->empty())
        return true;
    auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleHypothesis>(idx_granule);
    if (!granule)
        throw Exception(
            "Set index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);
    return mayBeTrue(expression_ast, granule->met).first;
}

MergeTreeIndexGranulePtr MergeTreeIndexHypothesis::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleHypothesis>(index.name);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexHypothesis::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorHypothesis>(index.name, index.sample_block.getNames().front());
}

MergeTreeIndexConditionPtr MergeTreeIndexHypothesis::createIndexCondition(
    const SelectQueryInfo & query, const Context & context) const
{
    return std::make_shared<MergeTreeIndexConditionHypothesis>(index.name, index.sample_block.getNames().front(), query, context);
}

bool MergeTreeIndexHypothesis::mayBenefitFromIndexForIn(const ASTPtr &) const
{
    return false;
}

MergeTreeIndexPtr hypothesisIndexCreator(const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexHypothesis>(index);
}

void hypothesisIndexValidator(const IndexDescription &, bool /*attach*/)
{
}


}
