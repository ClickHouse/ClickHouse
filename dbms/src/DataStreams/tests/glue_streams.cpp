#include <iostream>
#include <iomanip>

#include <statdaemons/threadpool.hpp>

#include <DB/IO/WriteBufferFromFileDescriptor.h>

#include <DB/Interpreters/loadMetadata.h>
#include <DB/Interpreters/executeQuery.h>

#include <DB/DataStreams/FormatFactory.h>
#include <DB/DataStreams/glueBlockInputStreams.h>


using Poco::SharedPtr;
using namespace DB;


void inputThread(BlockInputStreamPtr in, BlockOutputStreamPtr out, WriteBuffer & wb, Poco::FastMutex & mutex)
{
	while (Block block = in->read())
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		out->write(block);
		wb.next();
	}
}

void forkThread(ForkPtr fork)
{
	fork->run();
}


int main(int argc, char ** argv)
{
	try
	{
		Context context;

		context.setGlobalContext(context);
		context.setPath("./");

		loadMetadata(context);

		context.setCurrentDatabase("default");
		context.setSetting("max_threads", 1UL);

		BlockIO io1 = executeQuery(
			"SELECT SearchPhrase, count()"
				" FROM hits"
				" WHERE SearchPhrase != ''"
				" GROUP BY SearchPhrase"
				" ORDER BY count() DESC"
				" LIMIT 10",
			context, QueryProcessingStage::Complete);

		BlockIO io2 = executeQuery(
			"SELECT count()"
				" FROM hits"
				" WHERE SearchPhrase != ''",
			context, QueryProcessingStage::Complete);

		WriteBufferFromFileDescriptor wb(STDOUT_FILENO);

		BlockOutputStreamPtr out1 = context.getFormatFactory().getOutput("TabSeparated", wb, io1.in_sample);
		BlockOutputStreamPtr out2 = context.getFormatFactory().getOutput("TabSeparated", wb, io2.in_sample);

		BlockInputStreams inputs;
		inputs.push_back(io1.in);
		inputs.push_back(io2.in);

		for (size_t i = 0; i < inputs.size(); ++i)
			std::cerr << inputs[i]->getID() << std::endl;

		Forks forks;

		glueBlockInputStreams(inputs, forks);

		std::cerr << forks.size() << std::endl;

		Poco::FastMutex mutex;

		boost::threadpool::pool pool(inputs.size() + forks.size());

		pool.schedule(std::bind(inputThread, inputs[0], out1, std::ref(wb), std::ref(mutex)));
		pool.schedule(std::bind(inputThread, inputs[1], out2, std::ref(wb), std::ref(mutex)));

		for (size_t i = 0; i < forks.size(); ++i)
			pool.schedule(std::bind(forkThread, forks[i]));

		pool.wait();
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
