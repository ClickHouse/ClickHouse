#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromFile.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Interpreters/Context.h>
#include <unistd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_FILE_NAME;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int FILE_DOESNT_EXIST;
}

/// A function to read file as a string.
class FunctionFile : public IFunction
{
public:
    static constexpr auto name = "file";
    static FunctionPtr create(const Context &context) { return std::make_shared<FunctionFile>(context); }
    explicit FunctionFile(const Context &context_) : context(context_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isString(arguments[0].type))
            throw Exception(getName() + " is only implemented for types String", ErrorCodes::NOT_IMPLEMENTED);
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr column = arguments[0].column;
        const ColumnString * expected = checkAndGetColumn<ColumnString>(column.get());
        if (!expected)
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", arguments[0].column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);

        const ColumnString::Chars & chars = expected->getChars();
        const ColumnString::Offsets & offsets = expected->getOffsets();

        std::vector<String> checked_filenames(input_rows_count);

        auto result = ColumnString::create();
        auto & res_chars = result->getChars();
        auto & res_offsets = result->getOffsets();

        res_offsets.resize(input_rows_count);

        size_t source_offset = 0;
        size_t result_offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const char * filename = reinterpret_cast<const char *>(&chars[source_offset]);

            const String user_files_path = context.getUserFilesPath();
            String user_files_absolute_path = Poco::Path(user_files_path).makeAbsolute().makeDirectory().toString();
            Poco::Path poco_filepath = Poco::Path(filename);
            if (poco_filepath.isRelative())
                poco_filepath = Poco::Path(user_files_absolute_path, poco_filepath);
            const String file_absolute_path = poco_filepath.absolute().toString();
            checkReadIsAllowedOrThrow(user_files_absolute_path, file_absolute_path);

            checked_filenames[row] = file_absolute_path;
            auto file = Poco::File(file_absolute_path);

            if (!file.exists())
                throw Exception(fmt::format("File {} doesn't exist.", file_absolute_path), ErrorCodes::FILE_DOESNT_EXIST);

            const auto current_file_size = Poco::File(file_absolute_path).getSize();

            result_offset += current_file_size + 1;
            res_offsets[row] = result_offset;
            source_offset = offsets[row];
        }

        res_chars.resize(result_offset);

        size_t prev_offset = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            auto file_absolute_path = checked_filenames[row];
            ReadBufferFromFile in(file_absolute_path);
            char * res_buf = reinterpret_cast<char *>(&res_chars[prev_offset]);

            const size_t file_lenght = res_offsets[row] - prev_offset - 1;
            prev_offset = res_offsets[row];
            in.readStrict(res_buf, file_lenght);
            res_buf[file_lenght] = '\0';
        }

        return result;
    }

private:

    void checkReadIsAllowedOrThrow(const std::string & user_files_absolute_path, const std::string & file_absolute_path) const
    {
        // If run in Local mode, no need for path checking.
        if (context.getApplicationType() != Context::ApplicationType::LOCAL)
            if (file_absolute_path.find(user_files_absolute_path) != 0)
                throw Exception("File is not inside " + user_files_absolute_path, ErrorCodes::DATABASE_ACCESS_DENIED);

        Poco::File path_poco_file = Poco::File(file_absolute_path);
        if (path_poco_file.exists() && path_poco_file.isDirectory())
            throw Exception("File can't be a directory", ErrorCodes::INCORRECT_FILE_NAME);
    }

    const Context & context;
};


void registerFunctionFile(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFile>();
}

}
