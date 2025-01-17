#pragma once

#include <string_view>
#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Common/SettingsChanges.h>

namespace DB
{

class ASTCreateWorkloadQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    ASTPtr workload_name;
    ASTPtr workload_parent;

    /// Special version of settings that support optional `FOR resource` clause
    struct SettingChange
    {
        String name;
        Field value;
        String resource;

        SettingChange() = default;
        SettingChange(std::string_view name_, const Field & value_, std::string_view resource_) : name(name_), value(value_), resource(resource_) {}
        SettingChange(std::string_view name_, Field && value_, std::string_view resource_) : name(name_), value(std::move(value_)), resource(resource_) {}

        friend bool operator ==(const SettingChange & lhs, const SettingChange & rhs) { return (lhs.name == rhs.name) && (lhs.value == rhs.value) && (lhs.resource == rhs.resource); }
        friend bool operator !=(const SettingChange & lhs, const SettingChange & rhs) { return !(lhs == rhs); }
    };

    using SettingsChanges = std::vector<SettingChange>;
    SettingsChanges changes;

    bool or_replace = false;
    bool if_not_exists = false;

    String getID(char delim) const override { return "CreateWorkloadQuery" + (delim + getWorkloadName()); }

    ASTPtr clone() const override;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & format, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTCreateWorkloadQuery>(clone()); }

    String getWorkloadName() const;
    bool hasParent() const;
    String getWorkloadParent() const;

    QueryKind getQueryKind() const override { return QueryKind::Create; }
};

}
