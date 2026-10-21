#pragma once

#include <Columns/IColumn.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Core/Block.h>

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
            models.emplace_back(factory.get(*elem.type, hash(seed, elem.name), markov_model_params));
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
