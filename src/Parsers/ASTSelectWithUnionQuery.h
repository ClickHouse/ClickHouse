#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/SelectUnionMode.h>

namespace Poco::JSON { class Object; }

namespace DB
{
/** Single SELECT query or multiple SELECT queries with UNION
 * or UNION or UNION DISTINCT
  */
class ASTSelectWithUnionQuery : public ASTQueryWithOutput
{
public:
    String getID(char) const override { return "SelectWithUnionQuery"; }

    ASTPtr clone() const override;

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    /// Edge descriptors are valid for an unnormalized AST. A missing column-match vector means
    /// that every edge uses positional matching for backwards compatibility with older ASTs.
    SetOperationDescriptors getSetOperations() const;
    void setSetOperations(const SetOperationDescriptors & operations);

    /// Check this AST and all nested set-operation ASTs for BY NAME metadata.
    bool hasByNameSetOperation() const;

    QueryKind getQueryKind() const override { return QueryKind::Select; }

    SelectUnionMode union_mode{};
    SetOperationColumnMatchMode column_match_mode = SetOperationColumnMatchMode::Position;
    SelectUnionModes list_of_modes;
    SetOperationColumnMatchModes list_of_column_match_modes;
    bool is_normalized = false;

    ASTPtr list_of_selects;

    SelectUnionModesSet set_of_modes;

    /// Consider any mode other than ALL as non-default.
    bool hasNonDefaultUnionMode() const;

    bool hasQueryParameters() const;

    NameToNameMap getQueryParameters() const;

private:
    /// This variable is optional as we want to set it on the first call to hasQueryParameters
    /// and return the same variable on future calls to hasQueryParameters
    /// its mutable as we set it in const function
    mutable std::optional<bool> has_query_parameters;

};

}
