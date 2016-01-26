#pragma once

#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnVector.h>

#include <type_traits>

#if defined(__x86_64__)
	#define LIBDIVIDE_USE_SSE2 1
#endif

#include <libdivide.h>

namespace DB
{

template <typename T>
struct BlockFilterCreator
{
	static std::vector<IColumn::Filter> perform(const size_t num_rows, const IColumn * column,
		size_t num_shards, const std::vector<size_t> & slots)
	{
		const auto total_weight = slots.size();
		std::vector<IColumn::Filter> filters(num_shards);

		/** Деление отрицательного числа с остатком на положительное, в C++ даёт отрицательный остаток.
		  * Для данной задачи это не подходит. Поэтому, будем обрабатывать знаковые типы как беззнаковые.
		  * Это даёт уже что-то совсем не похожее на деление с остатком, но подходящее для данной задачи.
		  */
		using UnsignedT = typename std::make_unsigned<T>::type;

		/// const columns contain only one value, therefore we do not need to read it at every iteration
		if (column->isConst())
		{
			const auto data = typeid_cast<const ColumnConst<T> *>(column)->getData();
			const auto shard_num = slots[static_cast<UnsignedT>(data) % total_weight];

			for (size_t i = 0; i < num_shards; ++i)
				filters[i].assign(num_rows, static_cast<UInt8>(shard_num == i));
		}
		else
		{
			/// libdivide поддерживает только UInt32 или UInt64.
			using TUInt32Or64 = typename std::conditional<sizeof(UnsignedT) <= 4, UInt32, UInt64>::type;

			libdivide::divider<TUInt32Or64> divider(total_weight);

			const auto & data = typeid_cast<const ColumnVector<T> *>(column)->getData();

			/// NOTE Может быть, стоит поменять местами циклы.
			for (size_t i = 0; i < num_shards; ++i)
			{
				filters[i].resize(num_rows);
				for (size_t j = 0; j < num_rows; ++j)
					filters[i][j] = slots[
						static_cast<TUInt32Or64>(data[j]) - (static_cast<TUInt32Or64>(data[j]) / divider) * total_weight] == i;
			}
		}

		return filters;
	}
};

}
