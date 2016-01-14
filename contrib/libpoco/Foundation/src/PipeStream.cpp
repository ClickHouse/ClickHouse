//
// PipeStream.cpp
//
// $Id: //poco/1.4/Foundation/src/PipeStream.cpp#1 $
//
// Library: Foundation
// Package: Processes
// Module:  PipeStream
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/PipeStream.h"


namespace Poco {


//
// PipeStreamBuf
//


PipeStreamBuf::PipeStreamBuf(const Pipe& pipe, openmode mode): 
	BufferedStreamBuf(STREAM_BUFFER_SIZE, mode),
	_pipe(pipe)
{
}


PipeStreamBuf::~PipeStreamBuf()
{
}


int PipeStreamBuf::readFromDevice(char* buffer, std::streamsize length)
{
	return _pipe.readBytes(buffer, (int) length);
}


int PipeStreamBuf::writeToDevice(const char* buffer, std::streamsize length)
{
	return _pipe.writeBytes(buffer, (int) length);
}


void PipeStreamBuf::close()
{
	_pipe.close(Pipe::CLOSE_BOTH);
}


//
// PipeIOS
//


PipeIOS::PipeIOS(const Pipe& pipe, openmode mode):
	_buf(pipe, mode)
{
	poco_ios_init(&_buf);
}


PipeIOS::~PipeIOS()
{
	try
	{
		_buf.sync();
	}
	catch (...)
	{
	}
}


PipeStreamBuf* PipeIOS::rdbuf()
{
	return &_buf;
}


void PipeIOS::close()
{
	_buf.sync();
	_buf.close();
}


//
// PipeOutputStream
//


PipeOutputStream::PipeOutputStream(const Pipe& pipe):
	PipeIOS(pipe, std::ios::out),
	std::ostream(&_buf)
{
}


PipeOutputStream::~PipeOutputStream()
{
}


//
// PipeInputStream
//


PipeInputStream::PipeInputStream(const Pipe& pipe):
	PipeIOS(pipe, std::ios::in),
	std::istream(&_buf)
{
}


PipeInputStream::~PipeInputStream()
{
}


} // namespace Poco
