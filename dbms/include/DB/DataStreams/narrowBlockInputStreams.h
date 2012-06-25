#pragma once

#include <DB/DataStreams/ConcatBlockInputStream.h>


namespace DB
{

/** Если количество источников inputs больше width,
  *  то клеит источники друг с другом (с помощью ConcatBlockInputStream),
  *  чтобы количество источников стало не больше width.
  *
  * Старается клеить источники друг с другом равномерно.
  */
BlockInputStreams narrowBlockInputStreams(BlockInputStreams & inputs, size_t width)
{
	if (inputs.size() <= width)
		return inputs;

	std::vector<BlockInputStreams> partitions(width);

	/** Для распределения, используем генератор случайных чисел,
	  * но не инициализируем его, чтобы результат был детерминированным.
	  */
	drand48_data rand_buf = {{0}};
	long rand_res = 0;
	
	for (size_t i = 0, size = inputs.size(); i < size; ++i)
	{
		lrand48_r(&rand_buf, &rand_res);
		partitions[rand_res % width].push_back(inputs[i]);	/// Теоретически, не слишком хорошо.
	}

	BlockInputStreams res(width);
	for (size_t i = 0; i < width; ++i)
		res[i] = new ConcatBlockInputStream(partitions[i]);

	return res;
}

}
