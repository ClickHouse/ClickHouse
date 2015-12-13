#pragma once

#include <memory>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/WriteBufferFromFile.h>


namespace DB
{


/** Позволяет запустить команду,
  *  читать её stdout, stderr, писать в stdin,
  *  дождаться завершения.
  *
  * Реализация похожа на функцию popen из POSIX (посмотреть можно в исходниках libc).
  *
  * Наиболее важное отличие: использует vfork вместо fork.
  * Это сделано, потому что fork не работает (с ошибкой о нехватке памяти),
  *  при некоторых настройках overcommit-а, если размер адресного пространства процесса больше половины количества доступной памяти.
  * Также, изменение memory map-ов - довольно ресурсоёмкая операция.
  *
  * Второе отличие - позволяет работать одновременно и с stdin, и с stdout, и с stderr запущенного процесса,
  *  а также узнать код и статус завершения.
  */
class ShellCommand
{
private:
	pid_t pid;

	ShellCommand(pid_t pid, int in_fd, int out_fd, int err_fd)
		: pid(pid), in(in_fd), out(out_fd), err(err_fd) {};

	static std::unique_ptr<ShellCommand> executeImpl(const char * filename, char * const argv[]);

public:
	WriteBufferFromFile in;		/// Если команда читает из stdin, то не забудьте вызвать in.close() после записи туда всех данных.
	ReadBufferFromFile out;
	ReadBufferFromFile err;

	/// Выполнить команду с использованием /bin/sh -c
	static std::unique_ptr<ShellCommand> execute(const std::string & command);

	/// Выполнить исполняемый файл с указаннами аргументами. arguments - без argv[0].
	static std::unique_ptr<ShellCommand> executeDirect(const std::string & path, const std::vector<std::string> & arguments);

	/// Подождать завершения процесса, кинуть исключение, если код не 0 или если процесс был завершён не самостоятельно.
	void wait();

	/// Подождать завершения процесса, узнать код возврата. Кинуть исключение, если процесс был завершён не самостоятельно.
	int tryWait();
};


}
