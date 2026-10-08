#pragma once

#include <Parsers/IAST.h>

#include <map>

namespace Poco::JSON { class Object; }

namespace DB
{

class ASTColumns;
class ASTCreateQuery;
class ASTIdentifier;
class ASTStorage;

/// Storage and column overrides for a single table, for example:
///
///   TABLE OVERRIDE `foo` (PARTITION BY toYYYYMM(`createtime`))
///
class ASTTableOverride : public IAST
{
public:
    String table_name;
    ASTColumns * columns = nullptr;
    ASTStorage * storage = nullptr;
    bool is_standalone = true;
    String getID(char) const override { return "TableOverride " + table_name; }
    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override
    {
        f(reinterpret_cast<IAST **>(&columns), nullptr);
        f(reinterpret_cast<IAST **>(&storage), nullptr);
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

/// List of table overrides, for example:
///
///   TABLE OVERRIDE `foo` (PARTITION BY toYYYYMM(`createtime`)),
///   TABLE OVERRIDE `bar` (SAMPLE BY `id`)
///
class ASTTableOverrideList : public IAST
{
public:
    String getID(char) const override { return "TableOverrideList"; }
    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;
    void setTableOverride(const String & name, ASTPtr ast);
    void removeTableOverride(const String & name);
    ASTPtr tryGetTableOverride(const String & name) const;
    bool hasOverride(const String & name) const;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

private:
    std::map<String, size_t> positions;
};

}
