#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses a string like this:
  * {variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE]} | PROFILE 'profile_name'
  */
class ParserSettingsProfileElement : public IParserBase
{
public:
    ParserSettingsProfileElement & useIDMode(bool enable_) { id_mode = enable_; return *this; }
    ParserSettingsProfileElement & enableInheritKeyword(bool enable_) { enable_inherit_keyword = enable_; return *this; }

protected:
    const char * getName() const override { return "SettingsProfileElement"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool id_mode = false;
    bool enable_inherit_keyword = false;
};


class ParserSettingsProfileElements : public IParserBase
{
public:
    ParserSettingsProfileElements & useIDMode(bool enable_) { id_mode = enable_; return *this; }
    ParserSettingsProfileElements & enableInheritKeyword(bool enable_) { enable_inherit_keyword = enable_; return *this; }

protected:
    const char * getName() const override { return "SettingsProfileElements"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool id_mode = false;
    bool enable_inherit_keyword = false;
};

}
