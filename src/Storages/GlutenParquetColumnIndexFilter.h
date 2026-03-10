/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#if USE_PARQUET
#include <Core/Field.h>

namespace DB
{
/// For gluten native parquet
class GlutenParquetColumnIndexFilter
{
public:
    /// The expression is stored as Reverse Polish Notation.
    struct RPNElement
    {
        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_EQUALS,
            FUNCTION_NOT_EQUALS,
            FUNCTION_LESS,
            FUNCTION_GREATER,
            FUNCTION_LESS_OR_EQUALS,
            FUNCTION_GREATER_OR_EQUALS,
            FUNCTION_IN,
            FUNCTION_NOT_IN,
            FUNCTION_UNKNOWN, /// Can take any value.
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        explicit RPNElement(const Function function_ = FUNCTION_UNKNOWN) : function(function_) { }

        Function function = FUNCTION_UNKNOWN;
        std::string columnName;
        DB::Field value;
        DB::ColumnPtr column;
    };
};
}
#endif
