#pragma once

#include <Core/Names.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/PreparedSets.h>

#include <string>
#include <string_view>
#include <unordered_map>

namespace DB
{

class WriteBuffer;
class IQueryPlanStep;
class QueryPlan;

struct RuntimeFilterInfo
{
    String pretty_name;
    String build_column_name;
    String build_table_name;
};

struct PrettyColumnName
{
    String expression;
    String annotation;

    PrettyColumnName() = default;

    explicit PrettyColumnName(String expression_)
        : expression(std::move(expression_)) {}

    PrettyColumnName(String expression_, String annotation_)
        : expression(std::move(expression_)), annotation(std::move(annotation_)) {}
};

struct ExplainFormatSettings
{
    WriteBuffer & out;
    std::string header_prefix;
    std::string detail_prefix;
    size_t offset = 0;
    const size_t base_indent = 2;
    const char indent_char = ' ';
    const bool write_header = false;
    bool compact = false;
    bool pretty = false;
    std::unordered_map<String, PrettyColumnName> pretty_names;
    std::unordered_map<String, RuntimeFilterInfo> runtime_filter_names;
};

namespace QueryPlanFormat
{
    String trimColumnIdentifier(std::string_view name);
    void formatOutputColumns(const std::unordered_map<String, PrettyColumnName> & pretty_names, WriteBuffer & out, const IQueryPlanStep & step, const String & prefix);
    void formatJoinOutputColumns(WriteBuffer & out, const IQueryPlanStep & step, const String & prefix);

    String formatNodePretty(
        const ActionsDAG::Node * node,
        const std::unordered_map<String, PrettyColumnName> & pretty_names,
        const std::unordered_map<String, RuntimeFilterInfo> & runtime_filter_names,
        std::unordered_map<FutureSet::Hash, String, PreparedSets::Hashing> & subquery_set_names,
        int parent_precedence = 0);
    String formatColumnPretty(const String & column_name, const std::unordered_map<String, PrettyColumnName> & pretty_names);
    std::string_view getColumnAnnotation(const String & column_name, const ExplainFormatSettings & settings);

    void buildPrettyNamesMap(
        const QueryPlan & plan,
        std::unordered_map<String, PrettyColumnName> & pretty_names,
        std::unordered_map<String, RuntimeFilterInfo> & runtime_filter_names,
        std::unordered_map<FutureSet::Hash, String, PreparedSets::Hashing> & subquery_set_names);
}

}
