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
    ParserSettingsProfileElement & useIDMode(bool id_mode_ = true) { id_mode = id_mode_; return *this; }
    ParserSettingsProfileElement & useInheritKeyword(bool use_inherit_keyword_ = true) { use_inherit_keyword = use_inherit_keyword_; return *this; }

protected:
    const char * getName() const override { return "SettingsProfileElement"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool id_mode = false;
    bool use_inherit_keyword = false;
};


class ParserSettingsProfileElements : public IParserBase
{
public:
    ParserSettingsProfileElements & useIDMode(bool id_mode_ = true) { id_mode = id_mode_; return *this; }
    ParserSettingsProfileElements & useInheritKeyword(bool use_inherit_keyword_ = true) { use_inherit_keyword = use_inherit_keyword_; return *this; }

protected:
    const char * getName() const override { return "SettingsProfileElements"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool id_mode = false;
    bool use_inherit_keyword = false;
};

}
