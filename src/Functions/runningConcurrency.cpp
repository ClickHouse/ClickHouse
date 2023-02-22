#include <Columns/ColumnVector.h>
#include <Core/callOnTypeIndex.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/FormatSettings.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromString.h>
#include <base/defines.h>
#include <set>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int ILLEGAL_COLUMN;
        extern const int ILLEGAL_TYPE_OF_ARGUMENT;
        extern const int INCORRECT_DATA;
    }

    template <typename Name, typename ArgDataType, typename ConcurrencyDataType>
    class ExecutableFunctionRunningConcurrency : public IExecutableFunction
    {
    public:
        String getName() const override
        {
            return Name::name;
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            using ColVecArg = typename ArgDataType::ColumnType;
            const ColVecArg * col_begin = checkAndGetColumn<ColVecArg>(arguments[0].column.get());
            const ColVecArg * col_end   = checkAndGetColumn<ColVecArg>(arguments[1].column.get());
            if (!col_begin || !col_end)
                throw Exception("Constant columns are not supported at the moment",
                                ErrorCodes::ILLEGAL_COLUMN);
            const typename ColVecArg::Container & vec_begin = col_begin->getData();
            const typename ColVecArg::Container & vec_end   = col_end->getData();

            using ColVecConc = typename ConcurrencyDataType::ColumnType;
            typename ColVecConc::MutablePtr col_concurrency = ColVecConc::create(input_rows_count);
            typename ColVecConc::Container & vec_concurrency = col_concurrency->getData();

            std::multiset<typename ArgDataType::FieldType> ongoing_until;
            auto begin_serializaion = arguments[0].type->getDefaultSerialization();
            auto end_serialization = arguments[1].type->getDefaultSerialization();
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const auto begin = vec_begin[i];
                const auto end   = vec_end[i];

                if (unlikely(begin > end))
                {
                    const FormatSettings default_format;
                    WriteBufferFromOwnString buf_begin, buf_end;
                    begin_serializaion->serializeTextQuoted(*(arguments[0].column), i, buf_begin, default_format);
                    end_serialization->serializeTextQuoted(*(arguments[1].column), i, buf_end, default_format);
                    throw Exception(
                        "Incorrect order of events: " + buf_begin.str() + " > " + buf_end.str(),
                        ErrorCodes::INCORRECT_DATA);
                }

                ongoing_until.insert(end);

                // Erase all the elements from "ongoing_until" which
                // are less than or equal to "begin", i.e. durations
                // that have already ended. We consider "begin" to be
                // inclusive, and "end" to be exclusive.
                ongoing_until.erase(
                    ongoing_until.begin(), ongoing_until.upper_bound(begin));

                vec_concurrency[i] = ongoing_until.size();
            }

            return col_concurrency;
        }

        bool useDefaultImplementationForConstants() const override
        {
            return true;
        }
    };

    template <typename Name, typename ArgDataType, typename ConcurrencyDataType>
    class FunctionBaseRunningConcurrency : public IFunctionBase
    {
    public:
        explicit FunctionBaseRunningConcurrency(DataTypes argument_types_, DataTypePtr return_type_)
            : argument_types(std::move(argument_types_))
            , return_type(std::move(return_type_)) {}

        String getName() const override
        {
            return Name::name;
        }

        const DataTypes & getArgumentTypes() const override
        {
            return argument_types;
        }

        const DataTypePtr & getResultType() const override
        {
            return return_type;
        }

        ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
        {
            return std::make_unique<ExecutableFunctionRunningConcurrency<Name, ArgDataType, ConcurrencyDataType>>();
        }

        bool isStateful() const override
        {
            return true;
        }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    private:
        DataTypes argument_types;
        DataTypePtr return_type;
    };

    template <typename Name, typename ConcurrencyDataType>
    class RunningConcurrencyOverloadResolver : public IFunctionOverloadResolver
    {
        template <typename T>
        struct TypeTag
        {
            using Type = T;
        };

        /// Call a polymorphic lambda with a type tag of src_type.
        template <typename F>
        void dispatchForSourceType(const IDataType & src_type, F && f) const
        {
            WhichDataType which(src_type);

            switch (which.idx)
            {
            case TypeIndex::Date:       f(TypeTag<DataTypeDate>());       break;
            case TypeIndex::DateTime:   f(TypeTag<DataTypeDateTime>());   break;
            case TypeIndex::DateTime64: f(TypeTag<DataTypeDateTime64>()); break;
            default:
                throw Exception(
                    "Arguments for function " + getName() + " must be Date, DateTime, or DateTime64.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }

    public:
        static constexpr auto name = Name::name;

        static FunctionOverloadResolverPtr create(ContextPtr)
        {
            return std::make_unique<RunningConcurrencyOverloadResolver<Name, ConcurrencyDataType>>();
        }

        String getName() const override
        {
            return Name::name;
        }

        FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
        {
            // The type of the second argument must match with that of the first one.
            if (unlikely(!arguments[1].type->equals(*(arguments[0].type))))
            {
                throw Exception(
                    "Function " + getName() + " must be called with two arguments having the same type.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            DataTypes argument_types = { arguments[0].type, arguments[1].type };
            FunctionBasePtr base;
            dispatchForSourceType(*(arguments[0].type), [&](auto arg_type_tag) // Throws when the type is inappropriate.
            {
                using Tag = decltype(arg_type_tag);
                using ArgDataType = typename Tag::Type;

                base = std::make_unique<FunctionBaseRunningConcurrency<Name, ArgDataType, ConcurrencyDataType>>(argument_types, return_type);
            });

            return base;
        }

        DataTypePtr getReturnTypeImpl(const DataTypes &) const override
        {
            return std::make_shared<ConcurrencyDataType>();
        }

        size_t getNumberOfArguments() const override
        {
            return 2;
        }

        bool isInjective(const ColumnsWithTypeAndName &) const override
        {
            return false;
        }

        bool isStateful() const override
        {
            return true;
        }

        bool useDefaultImplementationForNulls() const override
        {
            return false;
        }
    };

    struct NameRunningConcurrency
    {
        static constexpr auto name = "runningConcurrency";
    };

    void registerFunctionRunningConcurrency(FunctionFactory & factory)
    {
        factory.registerFunction<RunningConcurrencyOverloadResolver<NameRunningConcurrency, DataTypeUInt32>>();
    }
}
