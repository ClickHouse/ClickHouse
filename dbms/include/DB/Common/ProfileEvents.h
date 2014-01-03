#pragma once

#include <stddef.h>


/** Позволяет считать количество различных событий, произошедших в программе
  *  - для высокоуровневого профайлинга.
  */

#define APPLY_FOR_EVENTS(M) \
	M(Query, 							"Queries") \
	M(SelectQuery, 						"Select queries") \
	M(InsertQuery, 						"Insert queries") \
	M(FileOpen, 						"File opens") \
	M(Seek, 							"Seeks") \
	M(ReadBufferFromFileDescriptorRead, "ReadBufferFromFileDescriptor reads") \
	M(ReadCompressedBytes, 				"Read compressed bytes") \
	M(CompressedReadBufferBlocks, 		"Read decompressed blocks") \
	M(CompressedReadBufferBytes, 		"Read decompressed bytes") \
	M(UncompressedCacheHits, 			"Uncompressed cache hits") \
	M(UncompressedCacheMisses, 			"Uncompressed cache misses") \
	\
	M(END, "")

namespace ProfileEvents
{
	/// Виды событий.
	enum Event
	{
	#define M(NAME, DESCRIPTION) NAME,
		APPLY_FOR_EVENTS(M)
	#undef M
	};


	/// Получить текстовое описание события по его enum-у.
	inline const char * getDescription(Event event)
	{
		static const char * descriptions[] =
		{
		#define M(NAME, DESCRIPTION) DESCRIPTION,
			APPLY_FOR_EVENTS(M)
		#undef M
		};

		return descriptions[event];
	}


	/// Счётчики - сколько раз каждое из событий произошло.
	extern size_t counters[Event::END];


	/// Увеличить счётчик события. Потокобезопасно.
	inline void increment(Event event, size_t amount = 1)
	{
		__sync_fetch_and_add(&counters[event], amount);
	}
}


#undef APPLY_FOR_EVENTS
