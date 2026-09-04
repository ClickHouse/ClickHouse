#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Analyzer/TableExpressionModifiers.h>

namespace DB
{

struct ReadFromTableWire;

class ReadFromTableStep : public ISourceStep
{
public:
    ReadFromTableStep(
        SharedHeader header, String table_name_, TableExpressionModifiers table_expression_modifiers_, bool use_parallel_replicas_ = false);

    String getName() const override { return "ReadFromTable"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `ReadFromTableStep.cpp` declares.
    ReadFromTableWire toWire() const;
    static QueryPlanStepPtr fromWire(ReadFromTableWire wire, Deserialization & ctx);

    const String & getTable() const { return table_name; }
    TableExpressionModifiers getTableExpressionModifiers() const { return table_expression_modifiers; }
    bool useParallelReplicas() const { return use_parallel_replicas; }
    bool & useParallelReplicas() { return use_parallel_replicas; }

    QueryPlanStepPtr clone() const override;
private:
    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
    String table_name;
    TableExpressionModifiers table_expression_modifiers;
    bool use_parallel_replicas = false;
};

/// What `ReadFromTableStep` puts on the wire in the framed format.
struct ReadFromTableWire
{
    String table;
    bool final = false;
    std::optional<TableExpressionModifiers::Rational> sample_size_ratio;
    std::optional<TableExpressionModifiers::Rational> sample_offset_ratio;
    bool use_parallel_replicas = false;
};

}
