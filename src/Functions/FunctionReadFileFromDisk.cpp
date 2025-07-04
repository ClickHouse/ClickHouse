#include <Functions/IFunction.h>

#include <Access/Common/AccessFlags.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Disks/IDisk.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class FunctionReadFileFromDisk : public IFunction, public WithContext
{
public:
    static constexpr auto name = "readFileFromDisk";
    static FunctionPtr create(ContextPtr context_)
    {
        context_->checkAccess(AccessType::readFileFromDisk);
        return std::make_shared<FunctionReadFileFromDisk>(context_);
    }

    explicit FunctionReadFileFromDisk(ContextPtr context_) : WithContext(context_) {}

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isDeterministic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2 || arguments.size() > 4)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Wrong argument count for function {}, expected from 2 to 4 arguments",
                getName());

        if (!WhichDataType(arguments[0]).isString())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected a string (disk name)",
                arguments[0]->getName(),
                getName());
    
        if (!WhichDataType(arguments[1]).isString())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected a string (path)",
                arguments[1]->getName(),
                getName());

        if (arguments.size() > 2 && !WhichDataType(arguments[2]).isNativeUInt())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of third argument of function {}, expected UInt64 (offset)",
                arguments[2]->getName(),
                getName());

        if (arguments.size() > 3 && !WhichDataType(arguments[3]).isNativeUInt())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of fourth argument of function {}, expected UInt64 (size)",
                arguments[3]->getName(),
                getName());
    
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnString * column_disk_name = nullptr;
        bool is_disk_name_const = false;
        {
            const IColumn * column_ptr = arguments[0].column.get();
            if (const ColumnConst * column_const = checkAndGetColumn<ColumnConst>(column_ptr))
            {
                column_ptr = &column_const->getDataColumn();
                is_disk_name_const = true;
            }
            column_disk_name = &checkAndGetColumn<ColumnString>(*column_ptr);
        }

        const ColumnString * column_path = nullptr;
        bool is_path_const = false;
        {
            const IColumn * column_ptr = arguments[1].column.get();
            if (const ColumnConst * column_const = checkAndGetColumn<ColumnConst>(column_ptr))
            {
                column_ptr = &column_const->getDataColumn();
                is_path_const = true;
            }
            column_path = &checkAndGetColumn<ColumnString>(*column_ptr);
        }

        const IColumn * column_offset = nullptr;
        bool is_offset_const = false;
        if (arguments.size() > 2)
        {
            column_offset = arguments[2].column.get();
            if (const ColumnConst * column_const = checkAndGetColumn<ColumnConst>(column_offset))
            {
                column_offset = &column_const->getDataColumn();
                is_offset_const = true;
            }
        }

        const IColumn * column_size = nullptr;
        bool is_size_const = false;
        if (arguments.size() > 3)
        {
            column_size = arguments[3].column.get();
            if (const ColumnConst * column_const = checkAndGetColumn<ColumnConst>(column_size))
            {
                column_size = &column_const->getDataColumn();
                is_size_const = true;
            }
        }

        auto result_column = ColumnString::create();
        auto & result_chars = result_column->getChars();
        auto & result_offsets = result_column->getOffsets();
        result_offsets.reserve(input_rows_count);

        DiskPtr disk;
        String file_name;
        std::unique_ptr<ReadBufferFromFileBase> file;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            StringRef disk_name = column_disk_name->getDataAt(is_disk_name_const ? 0 : i);
            StringRef path = column_path->getDataAt(is_path_const ? 0 : i);

            UInt64 offset = 0;
            if (column_offset)
                offset = column_offset->getUInt(is_offset_const ? 0 : i);

            std::optional<UInt64> size;
            if (column_size)
                size = column_size->getUInt(is_size_const ? 0 : i);

            if (!disk || (disk->getName() != disk_name))
            {
                file.reset();
                file_name.clear();
                disk = getContext()->getDisk(String{disk_name});
            }

            if (file_name != path)
            {
                file_name = String{path};
                file = disk->readFile(file_name, getContext()->getReadSettings());
            }
    
            file->seek(offset, SEEK_SET);

            if (size)
            {
                result_chars.reserve(result_chars.size() + *size);
                size_t read_bytes = file->readBig(reinterpret_cast<char *>(result_chars.data() + result_chars.size()), *size);
                result_chars.resize(result_chars.size() + read_bytes);
            }
            else
            {
                readStringUntilEOFInto(result_chars, *file);
            }
            
            result_chars.push_back(0);
            result_offsets.push_back(result_chars.size());
        }

        return result_column;
    }
};

}

REGISTER_FUNCTION(ReadFileFromDisk)
{
    factory.registerFunction<FunctionReadFileFromDisk>();
}

}
