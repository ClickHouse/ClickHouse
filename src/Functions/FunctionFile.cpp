#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
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
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int DATABASE_ACCESS_DENIED;
}

/// A function to read file as a string.
class FunctionFile : public IFunction, WithContext
{
public:
    static constexpr auto name = "file";
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionFile>(context_); }
    explicit FunctionFile(ContextPtr context_) : WithContext(context_) {}

    bool isVariadic() const override { return true; }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 1 or 2",
                getName(), arguments.size());

        if (!isString(arguments[0].type))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is only implemented for type String", getName());

        if (arguments.size() == 2)
        {
            if (arguments[1].type->onlyNull())
                return makeNullable(std::make_shared<DataTypeString>());

            if (!isString(arguments[1].type))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} only accepts String or Null as second argument", getName());
        }

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnPtr column = arguments[0].column;
        const ColumnString * column_src = checkAndGetColumn<ColumnString>(column.get());
        if (!column_src)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());

        String default_result;

        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;

        if (arguments.size() == 2)
        {
            if (result_type->isNullable())
            {
                col_null_map_to = ColumnUInt8::create(input_rows_count, false);
                vec_null_map_to = &col_null_map_to->getData();
            }
            else
            {
                const auto & default_column = arguments[1].column;
                const ColumnConst * default_col = checkAndGetColumn<ColumnConst>(default_column.get());

                if (!default_col)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                        arguments[1].column->getName(), getName());

                default_result = default_col->getValue<String>();
            }
        }

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
            std::string_view filename = column_src->getDataAt(row).toView();
            fs::path file_path(filename.data(), filename.data() + filename.size());

            if (file_path.is_relative())
                file_path = user_files_absolute_path / file_path;

            /// Do not use fs::canonical or fs::weakly_canonical.
            /// Otherwise it will not allow to work with symlinks in `user_files_path` directory.
            file_path = fs::absolute(file_path).lexically_normal();

            try
            {
                if (need_check && !file_path.string().starts_with(user_files_absolute_path_string))
                    throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "File is not inside {}", user_files_absolute_path.string());

                ReadBufferFromFile in(file_path);
                WriteBufferFromVector out(res_chars, AppendModeTag{});
                copyData(in, out);
                out.finalize();
            }
            catch (...)
            {
                if (arguments.size() == 1)
                    throw;

                if (vec_null_map_to)
                    (*vec_null_map_to)[row] = true;
                else
                    res_chars.insert(default_result.data(), default_result.data() + default_result.size());
            }

            res_chars.push_back(0);
            res_offsets[row] = res_chars.size();
        }

        if (vec_null_map_to)
            return ColumnNullable::create(std::move(result), std::move(col_null_map_to));

        return result;
    }
};


REGISTER_FUNCTION(File)
{
    factory.registerFunction<FunctionFile>();
}

}
