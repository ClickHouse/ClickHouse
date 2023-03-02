#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <unistd.h>
#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int DATABASE_ACCESS_DENIED;
}

/// A function to read file as a string.
class FunctionFile : public IFunction, WithContext
{
public:
    static constexpr auto name = "file";
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionFile>(context_); }
    explicit FunctionFile(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isString(arguments[0].type))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is only implemented for type String", getName());

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr column = arguments[0].column;
        const ColumnString * column_src = checkAndGetColumn<ColumnString>(column.get());
        if (!column_src)
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", arguments[0].column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);

        auto result = ColumnString::create();
        auto & res_chars = result->getChars();
        auto & res_offsets = result->getOffsets();

        res_offsets.resize(input_rows_count);

        fs::path user_files_absolute_path = fs::canonical(fs::path(getContext()->getUserFilesPath()));
        std::string user_files_absolute_path_string = user_files_absolute_path.string();

        // If run in Local mode, no need for path checking.
        bool need_check = getContext()->getApplicationType() != Context::ApplicationType::LOCAL;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            StringRef filename = column_src->getDataAt(row);
            fs::path file_path(filename.data, filename.data + filename.size);

            if (file_path.is_relative())
                file_path = user_files_absolute_path / file_path;

            /// Do not use fs::canonical or fs::weakly_canonical.
            /// Otherwise it will not allow to work with symlinks in `user_files_path` directory.
            file_path = fs::absolute(file_path).lexically_normal();

            if (need_check && file_path.string().find(user_files_absolute_path_string) != 0)
                throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "File is not inside {}", user_files_absolute_path.string());

            ReadBufferFromFile in(file_path);
            WriteBufferFromVector out(res_chars, AppendModeTag{});
            copyData(in, out);
            out.finalize();

            res_chars.push_back(0);
            res_offsets[row] = res_chars.size();
        }

        return result;
    }
};


void registerFunctionFile(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFile>();
}

}
