#include <Interpreters/Context.h>

#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnString.h>

#include <Poco/Path.h>
#include <Poco/File.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Common/StringUtils/StringUtils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int INCORRECT_DATA;
extern const int INCORRECT_FILE_NAME;
extern const int PATH_ACCESS_DENIED;
}

/** fileInc(x) - read integer from file, increment it and write it down. Create file with '0' if it doesn't exist.
  * Returns incremented integer
  */
class ExecutableFunctionFileInc : public IExecutableFunctionImpl
{
public:
    explicit ExecutableFunctionFileInc(const String & user_files_path_) : user_files_path{user_files_path_} {}

    static constexpr auto name = "fileInc";
    String getName() const override { return name; }

    void execute(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const IColumn * arg_column = columns[arguments[0]].column.get();
        const ColumnString * arg_string = checkAndGetColumnConstData<ColumnString>(arg_column);

        Poco::Path file_path = Poco::Path(arg_string->getDataAt(0).toString());
        if (file_path.isRelative())
            file_path = Poco::Path(user_files_path, file_path);

        if (!startsWith(file_path.toString(), user_files_path))
            throw Exception("File path " + file_path.toString() + " is not inside " + user_files_path, ErrorCodes::PATH_ACCESS_DENIED);

        Int64 file_val = 0;
        Poco::File file = Poco::File(file_path);

        if (file.exists())
        {
            if (file.isDirectory())
                throw Exception("File must not be a directory", ErrorCodes::INCORRECT_FILE_NAME);

            if (!file.canRead() || !file.canWrite())
                throw Exception("There is no RW access to file", ErrorCodes::PATH_ACCESS_DENIED);

            ReadBufferFromFile file_rb(file_path.toString(), WRITE_HELPERS_MAX_INT_WIDTH);

            if (!tryReadIntText(file_val, file_rb) || !file_rb.eof())
                throw Exception("File must contains only one integer", ErrorCodes::INCORRECT_DATA);

            file_val++;
        }
        else
        {
            Poco::File(file_path.parent()).createDirectories();
        }

        WriteBufferFromFile file_wb(file_path.toString(), WRITE_HELPERS_MAX_INT_WIDTH);
        writeIntText(file_val, file_wb);

        columns[result].column = DataTypeInt64().createColumnConst(input_rows_count, file_val);
    }

private:
    const String user_files_path;
};

class FunctionBaseFileInc : public IFunctionBaseImpl
{
public:
    explicit FunctionBaseFileInc(const String & user_files_path_, DataTypePtr return_type_)
        : user_files_path(user_files_path_), return_type(return_type_)
    {
    }

    static constexpr auto name = "fileInc";
    String getName() const override { return name; }

    const DataTypes & getArgumentTypes() const override
    {
        DataTypes argument_types(1);
        argument_types[0] = std::make_shared<DataTypeString>();

        return argument_types;
    }

    const DataTypePtr & getReturnType() const override { return return_type; }

    ExecutableFunctionImplPtr prepare(const ColumnsWithTypeAndName &, const ColumnNumbers &, size_t) const override
    {
        return std::make_unique<ExecutableFunctionFileInc>(user_files_path);
    }

private:
    const String user_files_path;
    DataTypePtr return_type;
};

class FunctionOverloadResolverFileInc : public IFunctionOverloadResolverImpl
{
public:
    explicit FunctionOverloadResolverFileInc(const String & user_files_path_): user_files_path(user_files_path_) {}

    static FunctionOverloadResolverImplPtr create(const Context & context)
    {
        return std::make_unique<FunctionOverloadResolverFileInc>(context.getUserFilesPath());
    }

    static constexpr auto name = "fileInc";
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnType(const DataTypes &) const override { return std::make_shared<DataTypeInt64>(); }

    FunctionBaseImplPtr build(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        if (!checkColumnConst<ColumnString>(arguments.at(0).column.get()))
            throw Exception("The argument of function " + getName() + " must be constant String", ErrorCodes::ILLEGAL_COLUMN);

        return std::make_unique<FunctionBaseFileInc>(user_files_path, return_type);
    }

private:
    const String user_files_path;
};

void registerFunctionFileInc(FunctionFactory & factory)
{
    factory.registerFunction<FunctionOverloadResolverFileInc>(FunctionFactory::CaseInsensitive);
}

}
