#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Names.h>
#include <Storages/ColumnDefault.h>
#include <Core/Block.h>
#include <Storages/ColumnCodec.h>
#include <optional>


namespace DB
{

struct ColumnDescription
{
    String name;
    DataTypePtr type;
    ColumnDefault default_desc;
    String comment;
    CompressionCodecPtr codec;
    ASTPtr ttl;

    ColumnDescription() = default;
    ColumnDescription(String name_, DataTypePtr type_) : name(std::move(name_)), type(std::move(type_)) {}

    bool operator==(const ColumnDescription & other) const;
    bool operator!=(const ColumnDescription & other) const { return !(*this == other); }

    void writeText(WriteBuffer & buf) const;
    void readText(ReadBuffer & buf);
};

class ColumnsDescription
{
public:
    ColumnsDescription() = default;
    explicit ColumnsDescription(NamesAndTypesList ordinary_);
    ColumnsDescription(const ColumnsDescription & other);
    ColumnsDescription & operator=(const ColumnsDescription & other);
    ColumnsDescription(ColumnsDescription &&) noexcept;
    ColumnsDescription & operator=(ColumnsDescription && other) noexcept;

    /// `after_column` can be a Nested column name;
    void add(ColumnDescription column, const String & after_column = String());
    /// `column_name` can be a Nested column name;
    void remove(const String & column_name);

    void flattenNested(); /// TODO: remove, insert already flattened Nested columns.

    bool operator==(const ColumnsDescription & other) const { return columns == other.columns; }
    bool operator!=(const ColumnsDescription & other) const { return !(*this == other); }

    std::list<ColumnDescription>::const_iterator begin() const { return columns.begin(); }
    std::list<ColumnDescription>::const_iterator end() const { return columns.end(); }

    NamesAndTypesList getOrdinary() const;
    NamesAndTypesList getMaterialized() const;
    NamesAndTypesList getAliases() const;
    /// ordinary + materialized + aliases.
    NamesAndTypesList getAll() const;

    using ColumnTTLs = std::unordered_map<String, ASTPtr>;
    ColumnTTLs getColumnTTLs() const;

    bool has(const String & column_name) const;
    bool hasNested(const String & column_name) const;
    ColumnDescription & get(const String & column_name);
    const ColumnDescription & get(const String & column_name) const;

    /// ordinary + materialized.
    NamesAndTypesList getAllPhysical() const;
    Names getNamesOfPhysical() const;
    bool hasPhysical(const String & column_name) const;
    NameAndTypePair getPhysical(const String & column_name) const;

    ColumnDefaults getDefaults() const; /// TODO: remove
    bool hasDefault(const String & column_name) const;
    std::optional<ColumnDefault> getDefault(const String & column_name) const;

    CompressionCodecPtr getCodecOrDefault(const String & column_name, CompressionCodecPtr default_codec) const;
    CompressionCodecPtr getCodecOrDefault(const String & column_name) const;

    String toString() const;
    static ColumnsDescription parse(const String & str);

    static const ColumnsDescription * loadFromContext(const Context & context, const String & db, const String & table);

private:
    std::list<ColumnDescription> columns;
    std::unordered_map<String, std::list<ColumnDescription>::iterator> name_to_column;
};

}
