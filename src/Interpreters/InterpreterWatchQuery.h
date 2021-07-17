#pragma once
/* Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

#include <Core/QueryProcessingStage.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using StoragePtr = std::shared_ptr<IStorage>;

class InterpreterWatchQuery : public IInterpreter, WithContext
{
public:
    InterpreterWatchQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;

    /// Table from where to read data, if not subquery.
    StoragePtr storage;
    /// Streams of read data
    BlockInputStreams streams;
};

}
