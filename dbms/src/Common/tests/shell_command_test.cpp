#include <iostream>
#include <DB/Common/ShellCommand.h>
#include <DB/IO/copyData.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/ReadBufferFromString.h>

using namespace DB;


int main(int arg, char ** argv)
try
{
	{
		auto command = ShellCommand::execute("echo 'Hello, world!'");

		WriteBufferFromFileDescriptor out(STDOUT_FILENO);
		copyData(command->out, out);

		command->wait();
	}

	{
		auto command = ShellCommand::executeDirect("/bin/echo", {"Hello, world!"});

		WriteBufferFromFileDescriptor out(STDOUT_FILENO);
		copyData(command->out, out);

		command->wait();
	}

	{
		auto command = ShellCommand::execute("cat");

		String in_str = "Hello, world!\n";
		ReadBufferFromString in(in_str);
		copyData(in, command->in);
		command->in.close();

		WriteBufferFromFileDescriptor out(STDOUT_FILENO);
		copyData(command->out, out);

		command->wait();
	}
}
catch (...)
{
	std::cerr << getCurrentExceptionMessage(false) << "\n";
	return 1;
}
