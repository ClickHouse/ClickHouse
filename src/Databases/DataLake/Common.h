#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/SettingsEnums.h>
#include <Core/Types.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
extern const int TABLE_ALREADY_EXISTS;
}

namespace DataLake
{

/// Thrown by a `CREATE TABLE` that registered nothing in the data lake catalog: the table name, or the
/// location the table would use, is already taken. `InterpreterCreateQuery` matches on this type rather
/// than on its `TABLE_ALREADY_EXISTS` code, which would also swallow unrelated exceptions from below.
class TableAlreadyExistsInCatalogException : public DB::Exception
{
public:
    template <typename... Args>
    explicit TableAlreadyExistsInCatalogException(FormatStringHelper<Args...> fmt, Args &&... args)
        : DB::Exception(DB::ErrorCodes::TABLE_ALREADY_EXISTS, std::move(fmt), std::forward<Args>(args)...)
    {
    }

    TableAlreadyExistsInCatalogException * clone() const override { return new TableAlreadyExistsInCatalogException(*this); }
    void rethrow() const override { throw *this; } /// NOLINT(bugprone-exception-copy-constructor-throws,cert-err60-cpp)

private:
    const char * name() const noexcept override { return "DataLake::TableAlreadyExistsInCatalogException"; }
    const char * className() const noexcept override { return "DataLake::TableAlreadyExistsInCatalogException"; }
};

String trim(const String & str);

std::vector<String> splitTypeArguments(const String & type_str);

DB::DataTypePtr getType(const String & type_name, bool nullable, const String & prefix = "");

/// Parse a string, containing at least one dot, into a two substrings:
/// A.B.C.D.E -> A.B.C.D and E, where
/// `A.B.C.D` is a table "namespace".
/// `E` is a table name.
std::pair<std::string, std::string> parseTableName(const std::string & name);

String constructTableLocation(
    const String & location_scheme,
    const String & storage_endpoint,
    const String & namespace_name,
    const String & table_name,
    DB::S3UriStyle uri_style = DB::S3UriStyle::AUTO);

}
