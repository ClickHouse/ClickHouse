#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Analyzer/TableExpressionModifiers.h>

namespace DB
{

struct ReadFromTableFunctionWire;

class ReadFromTableFunctionStep : public ISourceStep
{
public:
    ReadFromTableFunctionStep(SharedHeader header, std::string serialized_ast_, TableExpressionModifiers table_expression_modifiers_);

    String getName() const override { return "ReadFromTableFunction"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `ReadFromTableFunctionStep.cpp` declares.
    ReadFromTableFunctionWire toWire() const;
    static QueryPlanStepPtr fromWire(ReadFromTableFunctionWire wire, Deserialization & ctx);

    const std::string & getSerializedAST() const { return serialized_ast; }
    TableExpressionModifiers getTableExpressionModifiers() const { return table_expression_modifiers; }

private:
    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
    std::string serialized_ast;
    TableExpressionModifiers table_expression_modifiers;
};

/// What `ReadFromTableFunctionStep` puts on the wire in the framed format.
struct ReadFromTableFunctionWire
{
    String serialized_ast;
    bool final = false;
    std::optional<TableExpressionModifiers::Rational> sample_size_ratio;
    std::optional<TableExpressionModifiers::Rational> sample_offset_ratio;
};

}
