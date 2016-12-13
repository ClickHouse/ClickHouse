#pragma once
#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Interpreters/ProcessList.h>


namespace DB
{


/// Proxy class which counts number of written block, rows, bytes
class CountingBlockOutputStream : public IBlockOutputStream
{
public:

	CountingBlockOutputStream(const BlockOutputStreamPtr & stream_)
		: stream(stream_) {}

	void setProgressCallback(ProgressCallback callback)
	{
		progress_callback = callback;
	}

	void setProcessListElement(ProcessListElement * elem)
	{
		process_elem = elem;
	}

	const Progress & getProgress() const
	{
		return progress;
	}

	void write(const Block & block) override
	{
		stream->write(block);

		Progress local_progress(block.rowsInFirstColumn(), block.bytes(), 0);
		progress.incrementPiecewiseAtomically(local_progress);

		if (process_elem)
			process_elem->updateProgressOut(local_progress);

		if (progress_callback)
			progress_callback(local_progress);
	}

	void writePrefix() override 						{ stream->writePrefix(); }
	void writeSuffix() override 						{ stream->writeSuffix(); }
	void flush() override 								{ stream->flush(); }
	void onProgress(const Progress & progress) override { stream->onProgress(progress); }
	String getContentType() const override				{ return stream->getContentType(); }

protected:

	BlockOutputStreamPtr stream;
	Progress progress;
	ProgressCallback progress_callback;
	ProcessListElement * process_elem = nullptr;
};

}
