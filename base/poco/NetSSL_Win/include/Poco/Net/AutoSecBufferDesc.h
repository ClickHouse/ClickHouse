//
// AutoSecBufferDesc.h
//
// Library: NetSSL_Win
// Package: SSLCore
// Module:  AutoSecBufferDesc
//
// Definition of the AutoSecBufferDesc class.
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_AutoSecBufferDesc_INCLUDED
#define NetSSL_AutoSecBufferDesc_INCLUDED


#include "Poco/Net/NetSSL.h"
#include <windows.h>
#include <wincrypt.h>
#ifndef SECURITY_WIN32
#define SECURITY_WIN32
#endif
#include <sspi.h>


namespace Poco {
namespace Net {


template <int numBufs> 
class AutoSecBufferDesc: public SecBufferDesc
	/// AutoSecBufferDesc is a helper class for automatic initialization and release of SecBuffer objects.
{
public:
	AutoSecBufferDesc(SecurityFunctionTableW* pSec, bool autoRelease):
		/// Creates a AutoSecBufferDesc. If autoRelease is true, the buffers will be released with the provided pSec function.
		_pSec(pSec),
		_autoRelease(autoRelease)
	{
		poco_check_ptr (_pSec);
		poco_static_assert (numBufs > 0);
		
		initBuffers();
		cBuffers = numBufs;
		pBuffers = _buffers;
		ulVersion = SECBUFFER_VERSION;
	}

	~AutoSecBufferDesc()
		/// Destroys the AutoSecBufferDesc
	{
		release();
	}

	void reset(bool autoRelease)
	{
		release();
		_autoRelease = autoRelease;
		initBuffers();
		cBuffers  = numBufs;
		pBuffers  = _buffers;
		ulVersion = SECBUFFER_VERSION;
	}

	void release()
	{
		if (_autoRelease)
		{
			for (int i = 0; i < numBufs; ++i)
			{
				if (_buffers[i].pvBuffer)
				{
					_pSec->FreeContextBuffer(_buffers[i].pvBuffer);
				}
			}
			_autoRelease = false;
		}
	}

	SecBuffer& operator [] (Poco::UInt32 idx)
	{
		return _buffers[idx];
	}

	const SecBuffer& operator [] (Poco::UInt32 idx) const
	{
		return _buffers[idx];
	}

	void release(int idx)
		/// Will release the buffer if necessary
	{
		release(idx, _autoRelease);
	}

	void setSecBufferEmpty(int idx)
	{
		release(idx, _autoRelease);
	}

	void setSecBufferData(int idx, void* pData, int len)
	{
		setContent(idx, pData, len, SECBUFFER_DATA);
	}

	void setSecBufferToken(int idx, void* pData, int len)
	{
		setContent(idx, pData, len, SECBUFFER_TOKEN);
	}

	void setSecBufferStreamHeader(int idx, void* pData, int len)
	{
		setContent(idx, pData, len, SECBUFFER_STREAM_HEADER);
	}

	void setSecBufferStreamTrailer(int idx, void* pData, int len)
	{
		setContent(idx, pData, len, SECBUFFER_STREAM_TRAILER);
	}

private:
	AutoSecBufferDesc(const AutoSecBufferDesc& desc);
	AutoSecBufferDesc& operator = (const AutoSecBufferDesc& desc);

	void release(int idx, bool force)
	{
		if (force && _buffers[idx].pvBuffer)
			_pSec->FreeContextBuffer(_buffers[idx].pvBuffer);

		_buffers[idx].pvBuffer   = 0;
		_buffers[idx].cbBuffer   = 0;
		_buffers[idx].BufferType = SECBUFFER_EMPTY;
	}

	void initBuffers()
	{
		for (int i = 0; i < numBufs; ++i)
		{
			_buffers[i].pvBuffer   = 0;
			_buffers[i].cbBuffer   = 0;
			_buffers[i].BufferType = SECBUFFER_EMPTY;
		}
	}

	void setContent(int idx, void* pData, int len, unsigned long type)
	{
		release(idx, _autoRelease);
		_buffers[idx].pvBuffer   = pData;
		_buffers[idx].cbBuffer   = len;
		_buffers[idx].BufferType = type;
	}

private:
	SecurityFunctionTableW* _pSec;
	bool                    _autoRelease;
	SecBuffer               _buffers[numBufs];
};


} } // namespace Poco::Net


#endif // NetSSL_AutoSecBufferDesc_INCLUDED
