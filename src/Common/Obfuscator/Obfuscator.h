#pragma once

#include <Columns/IColumn.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Core/Block.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>

#include <Common/Obfuscator/Models.h>

namespace DB
{

class Obfuscator
{
private:
    std::vector<ModelPtr> models;

public:
    Obfuscator(const Block & header, UInt64 seed, MarkovModelParameters markov_model_params)
    {
        ModelFactory factory;

        size_t columns = header.columns();
        models.reserve(columns);

        for (const auto & elem : header)
        {
            try
            {
                models.emplace_back(factory.get(*elem.type, hash(seed, elem.name), markov_model_params));
            }
            catch (Exception & e)
            {
                /// The unsupported type may be nested deep inside the type of the column.
                e.addMessage("while creating the obfuscation model for column {} of type {}", backQuoteIfNeed(elem.name), elem.type->getName());
                throw;
            }
        }
    }

    void train(const Columns & columns)
    {
        size_t size = columns.size();
        for (size_t i = 0; i < size; ++i)
            models[i]->train(*columns[i]);
    }

    void finalize()
    {
        for (auto & model : models)
            model->finalize();
    }

    Columns generate(const Columns & columns)
    {
        size_t size = columns.size();
        Columns res(size);
        for (size_t i = 0; i < size; ++i)
            res[i] = models[i]->generate(*columns[i]);
        return res;
    }

    void updateSeed()
    {
        for (auto & model : models)
            model->updateSeed();
    }

    void serialize(WriteBuffer & out) const
    {
        for (const auto & model : models)
            model->serialize(out);
    }

    void deserialize(ReadBuffer & in)
    {
        for (auto & model : models)
            model->deserialize(in);
    }
};

}
