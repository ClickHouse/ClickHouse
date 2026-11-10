#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Analyzer/TableExpressionModifiers.h>

namespace DB
{

class ReadFromTableFunctionStep : public ISourceStep
{
public:
    ReadFromTableFunctionStep(SharedHeader header, std::string serialized_ast_, TableExpressionModifiers table_expression_modifiers_);

    String getName() const override { return "ReadFromTableFunction"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    const std::string & getSerializedAST() const { return serialized_ast; }
    TableExpressionModifiers getTableExpressionModifiers() const { return table_expression_modifiers; }

    /// Whether the storage of the table function needs the whole query, see `IStorage::readRequiresFullQuery`.
    void setReadsStorageRequiringFullQuery(bool value) { reads_storage_requiring_full_query = value; }
    bool readsStorageRequiringFullQuery() const { return reads_storage_requiring_full_query; }

private:
    std::string serialized_ast;
    TableExpressionModifiers table_expression_modifiers;
    bool reads_storage_requiring_full_query = false;
};

}
