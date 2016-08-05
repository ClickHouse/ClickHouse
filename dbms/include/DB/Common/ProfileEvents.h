#pragma once

#include <stddef.h>
#include <atomic>


/** Позволяет считать количество различных событий, произошедших в программе
  *  - для высокоуровневого профайлинга.
  */

#define APPLY_FOR_EVENTS(M) \
	M(Query) \
	M(SelectQuery) \
	M(InsertQuery) \
	M(FileOpen) \
	M(Seek) \
	M(ReadBufferFromFileDescriptorRead) \
	M(ReadBufferFromFileDescriptorReadBytes) \
	M(WriteBufferFromFileDescriptorWrite) \
	M(WriteBufferFromFileDescriptorWriteBytes) \
	M(ReadBufferAIORead) \
	M(ReadBufferAIOReadBytes) \
	M(WriteBufferAIOWrite) \
	M(WriteBufferAIOWriteBytes) \
	M(ReadCompressedBytes) \
	M(CompressedReadBufferBlocks) \
	M(CompressedReadBufferBytes) \
	M(UncompressedCacheHits) \
	M(UncompressedCacheMisses) \
	M(UncompressedCacheWeightLost) \
	M(IOBufferAllocs) \
	M(IOBufferAllocBytes) \
	M(ArenaAllocChunks) \
	M(ArenaAllocBytes) \
	M(FunctionExecute) \
	M(MarkCacheHits) \
	M(MarkCacheMisses) \
	M(CreatedReadBufferOrdinary) \
	M(CreatedReadBufferAIO) \
	M(CreatedWriteBufferOrdinary) \
	M(CreatedWriteBufferAIO) \
	\
	M(ReplicatedPartFetches) \
	M(ReplicatedPartFailedFetches) \
	M(ObsoleteReplicatedParts) \
	M(ReplicatedPartMerges) \
	M(ReplicatedPartFetchesOfMerged) \
	M(ReplicatedPartChecks) \
	M(ReplicatedPartChecksFailed) \
	M(ReplicatedDataLoss) \
	\
	M(DelayedInserts) \
	M(RejectedInserts) \
	M(DelayedInsertsMilliseconds) \
	M(SynchronousMergeOnInsert) \
	\
	M(ZooKeeperInit) \
	M(ZooKeeperTransactions) \
	M(ZooKeeperGetChildren) \
	M(ZooKeeperCreate) \
	M(ZooKeeperRemove) \
	M(ZooKeeperExists) \
	M(ZooKeeperGet) \
	M(ZooKeeperSet) \
	M(ZooKeeperMulti) \
	M(ZooKeeperExceptions) \
	\
	M(DistributedConnectionFailTry) \
	M(DistributedConnectionFailAtAll) \
	\
	M(CompileAttempt) \
	M(CompileSuccess) \
	\
	M(ExternalSortWritePart) \
	M(ExternalSortMerge) \
	M(ExternalAggregationWritePart) \
	M(ExternalAggregationMerge) \
	M(ExternalAggregationCompressedBytes) \
	M(ExternalAggregationUncompressedBytes) \
	\
	M(SlowRead) \
	M(ReadBackoff) \
	\
	M(ReplicaYieldLeadership) \
	M(ReplicaPartialShutdown) \
	M(ReplicaPermanentlyReadonly) \
	\
	M(SelectedParts) \
	M(SelectedRanges) \
	M(SelectedMarks) \
	\
	M(MergedRows) \
	M(MergedUncompressedBytes) \
	\
	M(MergeTreeDataWriterRows) \
	M(MergeTreeDataWriterUncompressedBytes) \
	M(MergeTreeDataWriterCompressedBytes) \
	M(MergeTreeDataWriterBlocks) \
	M(MergeTreeDataWriterBlocksAlreadySorted) \
	\
	M(END)

namespace ProfileEvents
{
	/// Виды событий.
	enum Event
	{
	#define M(NAME) NAME,
		APPLY_FOR_EVENTS(M)
	#undef M
	};


	/// Получить текстовое описание события по его enum-у.
	inline const char * getDescription(Event event)
	{
		static const char * descriptions[] =
		{
		#define M(NAME) #NAME,
			APPLY_FOR_EVENTS(M)
		#undef M
		};

		return descriptions[event];
	}


	/// Счётчики - сколько раз каждое из событий произошло.
	extern std::atomic<size_t> counters[END];


	/// Увеличить счётчик события. Потокобезопасно.
	inline void increment(Event event, size_t amount = 1)
	{
		counters[event] += amount;
	}
}


#undef APPLY_FOR_EVENTS
