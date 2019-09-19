#pragma once

#define CODE_TO_STR_IMPL_1(_0) \
    #_0 "\n"
#define CODE_TO_STR_IMPL_2(_0, _1) \
    #_0 ", " #_1 "\n"
#define CODE_TO_STR_IMPL_3(_0, _1, _2) \
    #_0 ", " #_1 ", " #_2 "\n"
#define CODE_TO_STR_IMPL_4(_0, _1, _2, _3) \
    #_0 ", " #_1 ", " #_2 ", " #_3 "\n"
#define CODE_TO_STR_IMPL_5(_0, _1, _2, _3, _4) \
    #_0 ", " #_1 ", " #_2 ", " #_3 ", " #_4 "\n"
#define CODE_TO_STR_IMPL_6(_0, _1, _2, _3, _4, _5) \
    #_0 ", " #_1 ", " #_2 ", " #_3 ", " #_4 ", " #_5 "\n"
#define CODE_TO_STR_IMPL_7(_0, _1, _2, _3, _4, _5, _6) \
    #_0 ", " #_1 ", " #_2 ", " #_3 ", " #_4 ", " #_5 ", " #_6 "\n"
#define CODE_TO_STR_IMPL_8(_0, _1, _2, _3, _4, _5, _6, _7) \
    #_0 ", " #_1 ", " #_2 ", " #_3 ", " #_4 ", " #_5 ", " #_6 ", " #_7 "\n"

#define CODE_TO_STR_SELECT(_0, _1, _2, _3, _4, _5, _6, _7, MACRO, ...) \
    MACRO

#define CODE_TO_STR(...) \
    CODE_TO_STR_SELECT( \
        __VA_ARGS__, \
        CODE_TO_STR_IMPL_8, \
        CODE_TO_STR_IMPL_7, \
        CODE_TO_STR_IMPL_6, \
        CODE_TO_STR_IMPL_5, \
        CODE_TO_STR_IMPL_4, \
        CODE_TO_STR_IMPL_3, \
        CODE_TO_STR_IMPL_2, \
        CODE_TO_STR_IMPL_1, \
        TOO_MANY_PARTS \
    )(__VA_ARGS__)

namespace DB
{

const char header_code[] {
    "#include <AggregateFunctions/IAggregateFunction.h>\n"
    "#include <Common/typeid_cast.h>\n"
    "#include <Core/Block.h>\n"
    "#include <Core/ColumnNumbers.h>\n"
    "#include <DataTypes/DataTypeArray.h>\n"
    "#include <DataTypes/DataTypesDecimal.h>\n"
    "#include <DataTypes/DataTypesNumber.h>\n"
    "#include <DataTypes/DataTypeString.h>\n"
    "#include <DataTypes/DataTypeTuple.h>\n"
    "\n"
    "#include <ext/range.h>\n"
    "\n"
    "\n"
    "namespace DB\n"
    "{\n"
    "\n"
    "namespace ErrorCodes\n"
    "{\n"
    "    extern const int ILLEGAL_COLUMN;\n"
    "}\n"
    "\n"
    CODE_TO_STR(
        // forward declarations

        template <typename T>
        bool arrayTypeCheck(const DataTypeArray * type);
        template <typename... T, size_t... I>
        bool tupleTypeCheck(
            const DataTypeTuple * type,
            std::index_sequence<I...>
        );
        template <typename T>
        std::vector<T> arrayToValue(const Array & array);
        template <typename... T, size_t... I>
        std::tuple<T...> tupleToValue(
            const Tuple & tuple,
            std::index_sequence<I...>
        );
        template <typename T>
        Array valueToArray(const std::vector<T> & value);
        template <typename... T, size_t... I>
        Tuple valueToTuple(
            const std::tuple<T...> & value,
            std::index_sequence<I...>
        );
    )
    CODE_TO_STR(
        // type checking

        bool fieldTypeCheck(const DataTypePtr & type, UInt8 *)
        {
            WhichDataType which {
                type
            };

            return which.isNativeUInt() || which.isDateOrDateTime();
        }

        bool fieldTypeCheck(const DataTypePtr & type, UInt16 *)
        {
            WhichDataType which {
                type
            };

            return which.isNativeUInt() || which.isDateOrDateTime();
        }

        bool fieldTypeCheck(const DataTypePtr & type, UInt32 *)
        {
            WhichDataType which {
                type
            };

            return which.isNativeUInt() || which.isDateOrDateTime();
        }

        bool fieldTypeCheck(const DataTypePtr & type, UInt64 *)
        {
            WhichDataType which {
                type
            };

            return which.isNativeUInt() || which.isDateOrDateTime();
        }

        bool fieldTypeCheck(const DataTypePtr & type, Int8 *)
        {
            WhichDataType which {
                type
            };

            return which.isNativeInt() || which.isEnum() || which.isInterval();
        }

        bool fieldTypeCheck(const DataTypePtr & type, Int16 *)
        {
            WhichDataType which {
                type
            };

            return which.isNativeInt() || which.isEnum() || which.isInterval();
        }

        bool fieldTypeCheck(const DataTypePtr & type, Int32 *)
        {
            WhichDataType which {
                type
            };

            return which.isNativeInt() || which.isEnum() || which.isInterval();
        }

        bool fieldTypeCheck(const DataTypePtr & type, Int64 *)
        {
            WhichDataType which {
                type
            };

            return which.isNativeInt() || which.isEnum() || which.isInterval();
        }

        bool fieldTypeCheck(const DataTypePtr & type, Float32 *)
        {
            WhichDataType which {
                type
            };

            return which.isFloat();
        }

        bool fieldTypeCheck(const DataTypePtr & type, Float64 *)
        {
            WhichDataType which {
                type
            };

            return which.isFloat();
        }

        bool fieldTypeCheck(const DataTypePtr & type, String *)
        {
            WhichDataType which {
                type
            };

            return which.isStringOrFixedString();
        }

        template <typename T>
        bool fieldTypeCheck(const DataTypePtr & type, std::vector<T> *)
        {
            return arrayTypeCheck<T>(
                typeid_cast<const DataTypeArray *>(type.get())
            );
        }

        template <typename... T>
        bool fieldTypeCheck(const DataTypePtr & type, std::tuple<T...> *)
        {
            return tupleTypeCheck<T...>(
                typeid_cast<const DataTypeTuple *>(type.get()),
                std::make_index_sequence<sizeof...(T)> {}
            );
        }

        template <typename T>
        bool arrayTypeCheck(const DataTypeArray * type)
        {
            return type
                && fieldTypeCheck(type->getNestedType(), (T *) nullptr);
        }

        template <typename... T, size_t... I>
        bool tupleTypeCheck(
            const DataTypeTuple * type,
            std::index_sequence<I...>
        )
        {
            return type
                && type->getElements().size() == sizeof...(T)
                && ...
                && fieldTypeCheck(type->getElements()[I], (T *) nullptr);
        }

        template <typename T>
        bool columnTypeCheck(
            bool is_const,
            const DataTypePtr & type,
            T *
        )
        {
            return is_const && fieldTypeCheck(type, (T *) nullptr);
        }

        template <typename T>
        bool columnTypeCheck(
            bool,
            const DataTypePtr & type,
            std::vector<T> *
        )
        {
            return fieldTypeCheck(type, (T *) nullptr);
        }
    )
    CODE_TO_STR(
        // db data to value

        template <typename T>
        typename std::enable_if<std::is_arithmetic<T>::value, T>::type
        fieldToValue(const Field & field, T *)
        {
            return field.get<T>();
        }

        String fieldToValue(const Field & field, String *)
        {
            return field.get<String>();
        }

        template <typename T>
        std::vector<T> fieldToValue(const Field & field, std::vector<T> *)
        {
            return arrayToValue<T>(
                field.get<Array>()
            );
        }

        template <typename... T>
        std::tuple<T...> fieldToValue(const Field & field, std::tuple<T...> *)
        {
            return tupleToValue<T...>(
                field.get<Tuple>(),
                std::make_index_sequence<sizeof...(T)> {}
            );
        }

        template <typename T>
        std::vector<T> arrayToValue(const Array & array)
        {
            std::vector<T> value;
            value.reserve(array.size());

            for (const Field & field : array)
                value.push_back(fieldToValue(field, (T *) nullptr));

            return value;
        }

        template <typename... T, size_t... I>
        std::tuple<T...> tupleToValue(
            const Tuple & tuple,
            std::index_sequence<I...>
        )
        {
            return {fieldToValue(tuple.toUnderType()[I], (T *) nullptr)...};
        }

        template <typename T>
        T columnToValue(
            const ColumnPtr & column,
            size_t,
            T *
        )
        {
            return fieldToValue((*column)[0], (T *) nullptr);
        }

        template <typename T>
        std::vector<T> columnToValue(
            const ColumnPtr & column,
            size_t size,
            std::vector<T> *
        )
        {
            // TODO: access columns directly?

            std::vector<T> value;
            value.reserve(size);

            for (size_t i : ext::range(0, size))
                value.push_back(fieldToValue((*column)[i], (T *) nullptr));

            return value;
        }
    )
    CODE_TO_STR(
        // type inference

        DataTypePtr fieldTypeInfer(UInt8 *)
        {
            return std::make_shared<DataTypeUInt8>();
        }

        DataTypePtr fieldTypeInfer(UInt16 *)
        {
            return std::make_shared<DataTypeUInt16>();
        }

        DataTypePtr fieldTypeInfer(UInt32 *)
        {
            return std::make_shared<DataTypeUInt32>();
        }

        DataTypePtr fieldTypeInfer(UInt64 *)
        {
            return std::make_shared<DataTypeUInt64>();
        }

        DataTypePtr fieldTypeInfer(Int8 *)
        {
            return std::make_shared<DataTypeInt8>();
        }

        DataTypePtr fieldTypeInfer(Int16 *)
        {
            return std::make_shared<DataTypeInt16>();
        }

        DataTypePtr fieldTypeInfer(Int32 *)
        {
            return std::make_shared<DataTypeInt32>();
        }

        DataTypePtr fieldTypeInfer(Int64 *)
        {
            return std::make_shared<DataTypeInt64>();
        }

        DataTypePtr fieldTypeInfer(Float32 *)
        {
            return std::make_shared<DataTypeFloat32>();
        }

        DataTypePtr fieldTypeInfer(Float64 *)
        {
            return std::make_shared<DataTypeFloat64>();
        }

        DataTypePtr fieldTypeInfer(String *)
        {
            return std::make_shared<DataTypeString>();
        }

        template <typename T>
        DataTypePtr fieldTypeInfer(std::vector<T> *)
        {
            return std::make_shared<DataTypeArray>(
                fieldTypeInfer((T *) nullptr)
            );
        }

        template <typename... T>
        DataTypePtr fieldTypeInfer(std::tuple<T...> *)
        {
            return std::make_shared<DataTypeTuple>(
                DataTypes {
                    fieldTypeInfer((T *) nullptr)...
                }
            );
        }

        template <typename T>
        DataTypePtr columnTypeInfer(T *)
        {
            return fieldTypeInfer((T *) nullptr);
        }

        template <typename T>
        DataTypePtr columnTypeInfer(std::vector<T> *)
        {
            return fieldTypeInfer((T *) nullptr);
        }
    )
    CODE_TO_STR(
        // value to db data

        template <typename T>
        typename std::enable_if<std::is_arithmetic<T>::value, Field>::type
        valueToField(const T & value)
        {
            return {value};
        }

        Field valueToField(const String & value)
        {
            return {value};
        }

        template <typename T>
        Field valueToField(const std::vector<T> & value)
        {
            return {valueToArray(value)};
        }

        template <typename... T>
        Field valueToField(const std::tuple<T...> & value)
        {
            return {valueToTuple(
                value,
                std::make_index_sequence<sizeof...(T)> {}
            )};
        }

        template <typename T>
        Array valueToArray(const std::vector<T> & value)
        {
            Array array;
            array.reserve(value.size());

            for (const T & i : value)
                array.push_back(valueToField(i));

            return array;
        }

        template <typename... T, size_t... I>
        Tuple valueToTuple(
            const std::tuple<T...> & value,
            std::index_sequence<I...>
        )
        {
            return Tuple {
                TupleBackend {
                    valueToField(std::get<I>(value))...
                }
            };
        }

        template <typename T>
        ColumnPtr valueToColumn(
            const T & value,
            const DataTypePtr & type,
            size_t size
        )
        {
            return type->createColumnConst(size, valueToField(value));
        }

        template <typename T>
        ColumnPtr valueToColumn(
            const std::vector<T> & value,
            const DataTypePtr & type,
            size_t size
        )
        {
            // TODO: access columns directly?

            MutableColumnPtr to {
                type->createColumn()
            };
            to->reserve(size);

            for (size_t i : ext::range(0, size))
                to->insert(valueToField(value[i]));

            return to;
        }
    )
    "\n"
    "}"
};

const char udf_code[] {
    "namespace DB\n"
    "{\n"
    "\n"
    CODE_TO_STR(
        template <typename Ret, typename... Arg>
        auto udfArgs(Ret (*)(size_t, Arg...))
        {
            return std::make_index_sequence<sizeof...(Arg)> {};
        }

        template <typename Ret, typename... Arg, size_t... I>
        DataTypePtr udfType(
            const ColumnsWithTypeAndName & arguments,
            Ret (*)(size_t, Arg...),
            std::index_sequence<I...>
        )
        {
            if (
                arguments.size() == sizeof...(Arg) + 1
                && ...
                && columnTypeCheck(
                    arguments[I + 1].column
                        && arguments[I + 1].column->isColumnConst(),
                    arguments[I + 1].type,
                    (Arg *) nullptr
                )
            )
                return columnTypeInfer((Ret *) nullptr);

            return nullptr;
        }

        template <typename Ret, typename... Arg, size_t... I>
        void udfExec(
            Block & block,
            const ColumnNumbers & arguments,
            size_t result_pos,
            size_t size,
            Ret (* func)(size_t, Arg...),
            std::index_sequence<I...>
        )
        {
            block.getByPosition(result_pos).column = valueToColumn(
                func(
                    size,
                    columnToValue(
                        block.getByPosition(
                            arguments[I + 1]
                        ).column,
                        size,
                        (Arg *) nullptr
                    )...
                ),
                block.getByPosition(result_pos).type,
                size
            );
        }

        extern "C" __attribute__ ((visibility ("default")))
        void funcType(
            DataTypePtr & result,
            const ColumnsWithTypeAndName & arguments
        )
        {
            result = udfType(
                arguments,
                ::udf,
                udfArgs(::udf)
            );
        }

        extern "C" __attribute__ ((visibility ("default")))
        void funcExec(
            Block & block,
            const ColumnNumbers & arguments,
            size_t result_pos,
            size_t input_rows_count
        )
        {
            udfExec(
                block,
                arguments,
                result_pos,
                input_rows_count,
                ::udf,
                udfArgs(::udf)
            );
        }
    )
    "\n"
    "}"
};

const char udsf_code[] {
    "namespace DB\n"
    "{\n"
    "\n"
    CODE_TO_STR(
        template <typename Ret, typename... Arg>
        auto udsfArgs(Ret (*)(Arg...))
        {
            return std::make_index_sequence<sizeof...(Arg)> {};
        }

        template <typename Ret, typename... Arg, size_t... I>
        DataTypePtr udsfType(
            const ColumnsWithTypeAndName & arguments,
            Ret (*)(Arg...),
            std::index_sequence<I...>
        )
        {
            if (
                arguments.size() == sizeof...(Arg) + 1
                && ...
                && fieldTypeCheck(arguments[I + 1].type, (Arg *) nullptr)
            )
                return fieldTypeInfer((Ret *) nullptr);

            return nullptr;
        }

        template <typename Ret, typename... Arg, size_t... I>
        void udsfExec(
            Block & block,
            const ColumnNumbers & arguments,
            size_t result_pos,
            size_t size,
            Ret (* func)(Arg...),
            std::index_sequence<I...>
        )
        {
            MutableColumnPtr to {
                block.getByPosition(result_pos).type->createColumn()
            };
            to->reserve(size);

            for (size_t i : ext::range(0, size))
                to->insert(
                    valueToField(
                        func(
                            fieldToValue(
                                (*block.getByPosition(
                                    arguments[I + 1]
                                ).column)[i],
                                (Arg *) nullptr
                            )...
                        )
                    )
                );

            block.getByPosition(result_pos).column = std::move(to);
        }

        extern "C" __attribute__ ((visibility ("default")))
        void funcType(
            DataTypePtr & result,
            const ColumnsWithTypeAndName & arguments
        )
        {
            result = udsfType(
                arguments,
                ::udsf,
                udsfArgs(::udsf)
            );
        }

        extern "C" __attribute__ ((visibility ("default")))
        void funcExec(
            Block & block,
            const ColumnNumbers & arguments,
            size_t result_pos,
            size_t input_rows_count
        )
        {
            udsfExec(
                block,
                arguments,
                result_pos,
                input_rows_count,
                ::udsf,
                udsfArgs(::udsf)
            );
        }
    )
    "\n"
    "}"
};

const char udaf_code[] {
    "namespace DB\n"
    "{\n"
    "\n"
    CODE_TO_STR(
        template <typename Data, typename... Arg>
        auto udafArgs(void (*)(Data &, Arg...))
        {
            return std::make_index_sequence<sizeof...(Arg)> {};
        }

        template <typename Ret, typename Data, typename... Arg, size_t... I>
        DataTypePtr udafType(
            const DataTypes & arguments,
            void (*)(Data &, Arg...),
            std::index_sequence<I...>,
            Ret (*)(const Data &)
        )
        {
            if (
                arguments.size() == sizeof...(Arg)
                && ...
                && fieldTypeCheck(arguments[I], (Arg *) nullptr)
            )
                return fieldTypeInfer((Ret *) nullptr);

            return nullptr;
        }

        template <typename Data, typename... Arg, size_t... I>
        void udafAdd(
            Data & data,
            const IColumn ** columns,
            size_t row_num,
            void (* func)(Data &, Arg...),
            std::index_sequence<I...>
        )
        {
            func(
                data,
                fieldToValue(
                    (*columns[I])[row_num],
                    (Arg *) nullptr
                )...
            );
        }

        extern "C" __attribute__ ((visibility ("default")))
        bool dataTrivial()
        {
            return std::is_trivially_destructible_v<Data>;
        }

        extern "C" __attribute__ ((visibility ("default")))
        size_t dataSize()
        {
            return sizeof(Data);
        }

        extern "C" __attribute__ ((visibility ("default")))
        size_t dataAlign()
        {
            return alignof(Data);
        }

        extern "C" __attribute__ ((visibility ("default")))
        void funcInit(AggregateDataPtr place)
        {
            new (place) Data;
        }

        extern "C" __attribute__ ((visibility ("default")))
        void funcFree(AggregateDataPtr place)
        {
            reinterpret_cast<Data *>(place)->~Data();
        }

        extern "C" __attribute__ ((visibility ("default")))
        void funcType(
            DataTypePtr & result,
            const DataTypes & arguments
        )
        {
            result = udafType(
                arguments,
                ::udafAdd,
                udafArgs(::udafAdd),
                ::udafReduce
            );
        }

        extern "C" __attribute__ ((visibility ("default")))
        void funcAdd(
            AggregateDataPtr place,
            const IColumn ** columns,
            size_t row_num
        )
        {
            udafAdd(
                *reinterpret_cast<Data *>(place),
                columns,
                row_num,
                ::udafAdd,
                udafArgs(::udafAdd)
            );
        }

        extern "C" __attribute__ ((visibility ("default")))
        void funcSerialize(
            ConstAggregateDataPtr place,
            WriteBuffer & buf
        )
        {
            Tuple tuple;

            tuple.toUnderType().push_back(
                valueToField(
                    *reinterpret_cast<const Data *>(place)
                )
            );
            writeBinary(tuple, buf);
        }

        extern "C" __attribute__ ((visibility ("default")))
        void funcDeserialize(
            AggregateDataPtr place,
            ReadBuffer & buf
        )
        {
            Tuple tuple;

            readBinary(tuple, buf);
            *reinterpret_cast<Data *>(place) = fieldToValue(
                tuple.toUnderType().front(),
                (Data *) nullptr
            );
        }

        extern "C" __attribute__ ((visibility ("default")))
        void funcMerge(
            AggregateDataPtr place,
            ConstAggregateDataPtr rhs
        )
        {
            ::udafMerge(
                *reinterpret_cast<Data *>(place),
                *reinterpret_cast<const Data *>(rhs)
            );
        }

        extern "C" __attribute__ ((visibility ("default")))
        void funcReduce(
            ConstAggregateDataPtr place,
            IColumn & to
        )
        {
            to.insert(
                valueToField(
                    ::udafReduce(
                        *reinterpret_cast<const Data *>(place)
                    )
                )
            );
        }
    )
    "\n"
    "}"
};

}

#undef CODE_TO_STR_IMPL_1
#undef CODE_TO_STR_IMPL_2
#undef CODE_TO_STR_IMPL_3
#undef CODE_TO_STR_IMPL_4
#undef CODE_TO_STR_IMPL_5
#undef CODE_TO_STR_IMPL_6
#undef CODE_TO_STR_IMPL_7
#undef CODE_TO_STR_IMPL_8
#undef CODE_TO_STR_SELECT
#undef CODE_TO_STR
