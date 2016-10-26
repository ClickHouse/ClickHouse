#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <dlfcn.h>

#include <DB/Common/Exception.h>
#include <DB/Common/ShellCommand.h>
#include <DB/IO/WriteBufferFromVector.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{
	namespace ErrorCodes
	{
		extern const int CANNOT_PIPE;
		extern const int CANNOT_DLSYM;
		extern const int CANNOT_FORK;
		extern const int CANNOT_WAITPID;
		extern const int CHILD_WAS_NOT_EXITED_NORMALLY;
		extern const int CANNOT_CREATE_CHILD_PROCESS;
	}
}


namespace
{
	struct Pipe
	{
		union
		{
			int fds[2];
			struct
			{
				int read_fd;
				int write_fd;
			};
		};

		Pipe()
		{
			#ifndef __APPLE__
			if (0 != pipe2(fds, O_CLOEXEC))
				DB::throwFromErrno("Cannot create pipe", DB::ErrorCodes::CANNOT_PIPE);
			#else
			if (0 != pipe(fds))
				DB::throwFromErrno("Cannot create pipe", DB::ErrorCodes::CANNOT_PIPE);
			if (0 != fcntl(fds[0], F_SETFD, FD_CLOEXEC))
				DB::throwFromErrno("Cannot create pipe", DB::ErrorCodes::CANNOT_PIPE);
			if (0 != fcntl(fds[1], F_SETFD, FD_CLOEXEC))
				DB::throwFromErrno("Cannot create pipe", DB::ErrorCodes::CANNOT_PIPE);
			#endif
		}

		~Pipe()
		{
			if (read_fd >= 0)
				close(read_fd);
			if (write_fd >= 0)
				close(write_fd);
		}
	};

	/// По этим кодам возврата из дочернего процесса мы узнаем (наверняка) об ошибках при его создании.
	enum class ReturnCodes : int
	{
		CANNOT_DUP_STDIN 	= 42,	/// Значение не принципиально, но выбрано так, чтобы редко конфликтовать с кодом возврата программы.
		CANNOT_DUP_STDOUT 	= 43,
		CANNOT_DUP_STDERR 	= 44,
		CANNOT_EXEC 		= 45,
	};
}


namespace DB
{


std::unique_ptr<ShellCommand> ShellCommand::executeImpl(const char * filename, char * const argv[])
{
	/** Тут написано, что при обычном вызове vfork, есть шанс deadlock-а в многопоточных программах,
	  *  из-за резолвинга символов в shared-библиотеке:
	  * http://www.oracle.com/technetwork/server-storage/solaris10/subprocess-136439.html
	  * Поэтому, отделим резолвинг символа от вызова.
	  */
	static void * real_vfork = dlsym(RTLD_DEFAULT, "vfork");

	if (!real_vfork)
		throwFromErrno("Cannot find symbol vfork in myself", ErrorCodes::CANNOT_DLSYM);

	Pipe pipe_stdin;
	Pipe pipe_stdout;
	Pipe pipe_stderr;

	pid_t pid = reinterpret_cast<pid_t(*)()>(real_vfork)();

	if (-1 == pid)
		throwFromErrno("Cannot vfork", ErrorCodes::CANNOT_FORK);

	if (0 == pid)
	{
		/// Находимся в свежесозданном процессе.

		/// Почему _exit а не exit? Потому что exit вызывает atexit и деструкторы thread local storage.
		/// А там куча мусора (в том числе, например, блокируется mutex). А это нельзя делать после vfork - происходит deadlock.

		/// Заменяем файловые дескрипторы на концы наших пайпов.
		if (STDIN_FILENO != dup2(pipe_stdin.read_fd, STDIN_FILENO))
			_exit(int(ReturnCodes::CANNOT_DUP_STDIN));

		if (STDOUT_FILENO != dup2(pipe_stdout.write_fd, STDOUT_FILENO))
			_exit(int(ReturnCodes::CANNOT_DUP_STDOUT));

		if (STDERR_FILENO != dup2(pipe_stderr.write_fd, STDERR_FILENO))
			_exit(int(ReturnCodes::CANNOT_DUP_STDERR));

		execv(filename, argv);
		/// Если процесс запущен, то execv не возвращает сюда.

		_exit(int(ReturnCodes::CANNOT_EXEC));
	}

	std::unique_ptr<ShellCommand> res(new ShellCommand(pid, pipe_stdin.write_fd, pipe_stdout.read_fd, pipe_stderr.read_fd));

	/// Теперь владение файловыми дескрипторами передано в результат.
	pipe_stdin.write_fd = -1;
	pipe_stdout.read_fd = -1;
	pipe_stderr.read_fd = -1;

	return res;
}


std::unique_ptr<ShellCommand> ShellCommand::execute(const std::string & command)
{
	/// Аргументы в неконстантных кусках памяти (как требуется для execv).
	/// Причём, их копирование должно быть совершено раньше вызова vfork, чтобы после vfork делать минимум вещей.
	std::vector<char> argv0("sh", "sh" + strlen("sh") + 1);
	std::vector<char> argv1("-c", "-c" + strlen("-c") + 1);
	std::vector<char> argv2(command.data(), command.data() + command.size() + 1);

	char * const argv[] = { argv0.data(), argv1.data(), argv2.data(), nullptr };

	return executeImpl("/bin/sh", argv);
}


std::unique_ptr<ShellCommand> ShellCommand::executeDirect(const std::string & path, const std::vector<std::string> & arguments)
{
	size_t argv_sum_size = path.size() + 1;
	for (const auto & arg : arguments)
		argv_sum_size += arg.size() + 1;

	std::vector<char *> argv(arguments.size() + 2);
	std::vector<char> argv_data(argv_sum_size);
	WriteBuffer writer(argv_data.data(), argv_sum_size);

	argv[0] = writer.position();
	writer.write(path.data(), path.size() + 1);

	for (size_t i = 0, size = arguments.size(); i < size; ++i)
	{
		argv[i + 1] = writer.position();
		writer.write(arguments[i].data(), arguments[i].size() + 1);
	}

	argv[arguments.size() + 1] = nullptr;

	return executeImpl(path.data(), argv.data());
}


int ShellCommand::tryWait()
{
	int status = 0;
	if (-1 == waitpid(pid, &status, 0))
		throwFromErrno("Cannot waitpid", ErrorCodes::CANNOT_WAITPID);

	if (WIFEXITED(status))
		return WEXITSTATUS(status);

	if (WIFSIGNALED(status))
		throw Exception("Child process was terminated by signal " + toString(WTERMSIG(status)), ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY);

	if (WIFSTOPPED(status))
		throw Exception("Child process was stopped by signal " + toString(WSTOPSIG(status)), ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY);

	throw Exception("Child process was not exited normally by unknown reason", ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY);
}


void ShellCommand::wait()
{
	int retcode = tryWait();

	if (retcode != EXIT_SUCCESS)
	{
		switch (retcode)
		{
			case int(ReturnCodes::CANNOT_DUP_STDIN):
				throw Exception("Cannot dup2 stdin of child process", ErrorCodes::CANNOT_CREATE_CHILD_PROCESS);
			case int(ReturnCodes::CANNOT_DUP_STDOUT):
				throw Exception("Cannot dup2 stdout of child process", ErrorCodes::CANNOT_CREATE_CHILD_PROCESS);
			case int(ReturnCodes::CANNOT_DUP_STDERR):
				throw Exception("Cannot dup2 stderr of child process", ErrorCodes::CANNOT_CREATE_CHILD_PROCESS);
			case int(ReturnCodes::CANNOT_EXEC):
				throw Exception("Cannot execv in child process", ErrorCodes::CANNOT_CREATE_CHILD_PROCESS);
			default:
				throw Exception("Child process was exited with return code " + toString(retcode), ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY);
		}
	}
}


}
