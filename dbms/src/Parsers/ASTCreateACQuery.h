#pragma once

#include <Parsers/IAST.h>


namespace DB
{
class ASTRoleName : public IAST
{
public:
    String name;
    String domain;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

using ASTUserName = ASTRoleName;


class ASTAuthentication : public IAST
{
public:
    enum Type
    {
        NO_PASSWORD,
        PLAINTEXT_PASSWORD,
        SHA256_PASSWORD,
        SHA256_HASH,
    };
    Type type = NO_PASSWORD;
    ASTPtr password;
    bool alter = false;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};


class ASTAllowedHosts : public IAST
{
public:
    ASTs host_names;
    ASTs host_regexps;
    ASTs ip_addresses;
    bool any_host = false;

    bool alter = false;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};


class ASTDefaultRoles : public IAST
{
public:
    ASTs role_names;

    struct Alter
    {
        bool all_granted = false;
    };
    std::optional<Alter> alter;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};


class ASTSettingsWithConstraint : public IAST
{
public:
    struct Entry
    {
        String name;
        ASTPtr value;
        ASTPtr min;
        ASTPtr max;
        bool readonly = false;
    };
    std::vector<Entry> set_list;

    bool alter = false;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};


class ASTAccountLock : public IAST
{
public:
    bool locked = false;

    bool alter = false;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};


class ASTCreateRoleQuery : public IAST
{
public:
    bool if_not_exists = false;
    ASTs role_names;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};


class ASTCreateUserQuery : public IAST
{
public:
    bool if_not_exists = false;
    ASTPtr user_name;
    ASTPtr authentication;
    ASTPtr allowed_hosts;
    ASTPtr default_roles;
    ASTPtr settings_with_constraints;
    ASTPtr account_lock;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}

