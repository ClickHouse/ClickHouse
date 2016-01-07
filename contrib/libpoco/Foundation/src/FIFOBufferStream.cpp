//
// FIFOBufferStream.cpp
//
// $Id: //poco/1.4/Foundation/src/FIFOBufferStream.cpp#1 $
//
// Library: Foundation
// Package: Streams
// Module:  FIFOBufferStream
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/FIFOBufferStream.h"


namespace Poco {


//
// FIFOBufferStreamBuf
//


FIFOBufferStreamBuf::FIFOBufferStreamBuf():
	BufferedBidirectionalStreamBuf(STREAM_BUFFER_SIZE + 4, std::ios::in | std::ios::out),
	_pFIFOBuffer(new FIFOBuffer(STREAM_BUFFER_SIZE, true)),
	_fifoBuffer(*_pFIFOBuffer)
{
}


FIFOBufferStreamBuf::FIFOBufferStreamBuf(FIFOBuffer& fifoBuffer):
	BufferedBidirectionalStreamBuf(fifoBuffer.size() + 4, std::ios::in | std::ios::out),
	_pFIFOBuffer(0),
	_fifoBuffer(fifoBuffer)
{
	fifoBuffer.setNotify(true);
}


FIFOBufferStreamBuf::FIFOBufferStreamBuf(char* pBuffer, std::size_t length):
	BufferedBidirectionalStreamBuf(length + 4, std::ios::in | std::ios::out),
	_pFIFOBuffer(new FIFOBuffer(pBuffer, length, true)),
	_fifoBuffer(*_pFIFOBuffer)
{
}


FIFOBufferStreamBuf::FIFOBufferStreamBuf(const char* pBuffer, std::size_t length):
	BufferedBidirectionalStreamBuf(length + 4, std::ios::in | std::ios::out),
	_pFIFOBuffer(new FIFOBuffer(pBuffer, length, true)),
	_fifoBuffer(*_pFIFOBuffer)
{
}


FIFOBufferStreamBuf::FIFOBufferStreamBuf(std::size_t length):
	BufferedBidirectionalStreamBuf(length + 4, std::ios::in | std::ios::out),
	_pFIFOBuffer(new FIFOBuffer(length, true)),
	_fifoBuffer(*_pFIFOBuffer)
{
}


FIFOBufferStreamBuf::~FIFOBufferStreamBuf()
{
	delete _pFIFOBuffer;
}


int FIFOBufferStreamBuf::readFromDevice(char* buffer, std::streamsize length)
{
	poco_assert (length > 0);
	return static_cast<int>(_fifoBuffer.read(buffer, static_cast<std::size_t>(length)));
}


int FIFOBufferStreamBuf::writeToDevice(const char* buffer, std::streamsize length)
{
	poco_assert (length > 0);
	return static_cast<int>(_fifoBuffer.write(buffer, static_cast<std::size_t>(length)));
}


//
// FIFOIOS
//


FIFOIOS::FIFOIOS(FIFOBuffer& fifoBuffer): _buf(fifoBuffer)
{
	poco_ios_init(&_buf);
}


FIFOIOS::FIFOIOS(char* pBuffer, std::size_t length): _buf(pBuffer, length)
{
	poco_ios_init(&_buf);
}


FIFOIOS::FIFOIOS(const char* pBuffer, std::size_t length): _buf(pBuffer, length)
{
	poco_ios_init(&_buf);
}


FIFOIOS::FIFOIOS(std::size_t length): _buf(length)
{
	poco_ios_init(&_buf);
}


FIFOIOS::~FIFOIOS()
{
	try
	{
		_buf.sync();
	}
	catch (...)
	{
	}
}


FIFOBufferStreamBuf* FIFOIOS::rdbuf()
{
	return &_buf;
}


void FIFOIOS::close()
{
	_buf.sync();
}


//
// FIFOBufferStream
//


FIFOBufferStream::FIFOBufferStream(FIFOBuffer& fifoBuffer):
	FIFOIOS(fifoBuffer),
    std::iostream(&_buf),
	readable(_buf.fifoBuffer().readable),
	writable(_buf.fifoBuffer().writable)
{
}


FIFOBufferStream::FIFOBufferStream(char* pBuffer, std::size_t length):
	FIFOIOS(pBuffer, length),
    std::iostream(&_buf),
	readable(_buf.fifoBuffer().readable),
	writable(_buf.fifoBuffer().writable)
{
}


FIFOBufferStream::FIFOBufferStream(const char* pBuffer, std::size_t length):
	FIFOIOS(pBuffer, length),
    std::iostream(&_buf),
	readable(_buf.fifoBuffer().readable),
	writable(_buf.fifoBuffer().writable)
{
}


FIFOBufferStream::FIFOBufferStream(std::size_t length):
	FIFOIOS(length),
    std::iostream(&_buf),
	readable(_buf.fifoBuffer().readable),
	writable(_buf.fifoBuffer().writable)
{
}


FIFOBufferStream::~FIFOBufferStream()
{
}


} // namespace Poco
