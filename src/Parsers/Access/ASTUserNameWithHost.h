#pragma once

#include <Parsers/IAST.h>

namespace DB
{

/** Represents a user name.
  * It can be a simple string or identifier or something like `name@host`.
  * In the last case `host` specifies the hosts user is allowed to connect from.
  * The `host` can be an ip address, ip subnet, or a host name.
  * The % and _ wildcard characters are permitted in `host`.
  * These have the same meaning as for pattern-matching operations performed with the LIKE operator.
  */
class ASTUserNameWithHost : public IAST
{
public:
    explicit ASTUserNameWithHost(const String & name_);
    explicit ASTUserNameWithHost(ASTPtr && name_ast_, String && host_pattern_ = "");

    String getBaseName() const;
    String getHostPattern() const;

    String toString() const;

    String getID(char) const override { return "UserNameWithHost"; }
    ASTPtr clone() const override;
    void replace(String name_);

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const override;

private:
    String getStringFromAST(const ASTPtr & ast) const;

    ASTPtr username;
    ASTPtr host_pattern;
};


class ASTUserNamesWithHost : public IAST
{
public:
    ASTUserNamesWithHost() = default;
    explicit ASTUserNamesWithHost(const String & name_);

    size_t size() const { return children.size(); }
    auto begin() const { return children.begin(); }
    auto end() const { return children.end(); }

    Strings toStrings() const;
    bool getHostPatternIfCommon(String & out_common_host_pattern) const;

    String getID(char) const override { return "UserNamesWithHost"; }
    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTUserNamesWithHost>(*this);
        clone->cloneChildren();
        return clone;
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
