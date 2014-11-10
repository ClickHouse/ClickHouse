#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/copyData.h>


namespace DB
{

/** Пустой поток блоков.
  * Но при первой попытке чтения, копирует данные из переданного input-а в переданный output.
  * Это нужно для выполнения запроса INSERT SELECT - запрос копирует данные, но сам ничего не возвращает.
  * Запрос можно было бы выполнять и без оборачивания в пустой BlockInputStream,
  *  но не работал бы прогресс выполнения запроса и возможность отменить запрос.
  */
class NullAndDoCopyBlockInputStream : public IProfilingBlockInputStream
{
public:
	NullAndDoCopyBlockInputStream(BlockInputStreamPtr input_, BlockOutputStreamPtr output_)
		: input(input_), output(output_)
	{
		children.push_back(input_);
	}

	String getName() const override { return "NullAndDoCopyBlockInputStream"; }

	String getID() const override
	{
		std::stringstream res;
		res << "copy from " << input->getID();
		return res.str();
	}

protected:
	Block readImpl() override
	{
		copyData(*input, *output);
		return Block();
	}

private:
	BlockInputStreamPtr input;
	BlockOutputStreamPtr output;
};

}
