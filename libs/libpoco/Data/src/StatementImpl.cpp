//
// StatementImpl.cpp
//
// $Id: //poco/Main/Data/src/StatementImpl.cpp#20 $
//
// Library: Data
// Package: DataCore
// Module:  StatementImpl
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/StatementImpl.h"
#include "Poco/Data/SessionImpl.h"
#include "Poco/Data/DataException.h"
#include "Poco/Data/AbstractBinder.h"
#include "Poco/Data/Extraction.h"
#include "Poco/Data/LOB.h"
#include "Poco/Data/Date.h"
#include "Poco/Data/Time.h"
#include "Poco/SharedPtr.h"
#include "Poco/DateTime.h"
#include "Poco/Exception.h"
#include "Poco/Data/DataException.h"


using Poco::icompare;


namespace Poco {
namespace Data {


using namespace Keywords;


const std::string StatementImpl::VECTOR = "vector";
const std::string StatementImpl::LIST = "list";
const std::string StatementImpl::DEQUE = "deque";
const std::string StatementImpl::UNKNOWN = "unknown";


StatementImpl::StatementImpl(SessionImpl& rSession):
	_state(ST_INITIALIZED),
	_extrLimit(upperLimit(Limit::LIMIT_UNLIMITED, false)),
	_lowerLimit(0),
	_rSession(rSession),
	_storage(STORAGE_UNKNOWN_IMPL),
	_ostr(),
	_curDataSet(0),
	_bulkBinding(BULK_UNDEFINED),
	_bulkExtraction(BULK_UNDEFINED)
{
	if (!_rSession.isConnected())
		throw NotConnectedException(_rSession.connectionString());

	_extractors.resize(1);
	_columnsExtracted.resize(1, 0);
	_subTotalRowCount.resize(1, 0);
}


StatementImpl::~StatementImpl()
{
}


std::size_t StatementImpl::execute(const bool& reset)
{
	if (reset) resetExtraction();

	if (!_rSession.isConnected())
	{
		_state = ST_DONE;
		throw NotConnectedException(_rSession.connectionString());
	}

	std::size_t lim = 0;
	if (_lowerLimit > _extrLimit.value())
		throw LimitException("Illegal Statement state. Upper limit must not be smaller than the lower limit.");

	do
	{
		compile();
		if (_extrLimit.value() == Limit::LIMIT_UNLIMITED)
			lim += executeWithoutLimit();
		else
			lim += executeWithLimit();
	} while (canCompile());

	if (_extrLimit.value() == Limit::LIMIT_UNLIMITED)
		_state = ST_DONE;

	if (lim < _lowerLimit)
		throw LimitException("Did not receive enough data.");

	assignSubTotal(reset);

	return lim;
}


void StatementImpl::assignSubTotal(bool reset)
{
	if (_extractors.size() == _subTotalRowCount.size())
	{
		CountVec::iterator it  = _subTotalRowCount.begin();
		CountVec::iterator end = _subTotalRowCount.end();
		for (int counter = 0; it != end; ++it, ++counter)
		{
			if (_extractors[counter].size())
			{
				if (reset)
					*it += CountVec::value_type(_extractors[counter][0]->numOfRowsHandled());
				else
					*it = CountVec::value_type(_extractors[counter][0]->numOfRowsHandled());
			}
		}
	}
}


std::size_t StatementImpl::executeWithLimit()
{
	poco_assert (_state != ST_DONE);
	std::size_t count = 0;
	std::size_t limit = _extrLimit.value();

	do
	{
		bind();
		while (count < limit && hasNext()) 
			count += next();
	} while (count < limit && canBind());

	if (!canBind() && (!hasNext() || limit == 0)) 
		_state = ST_DONE;
	else if (hasNext() && limit == count && _extrLimit.isHardLimit())
		throw LimitException("HardLimit reached (retrieved more data than requested).");
	else 
		_state = ST_PAUSED;

	int affectedRows = affectedRowCount();
	if (count == 0)
	{
		if (affectedRows > 0)
			return affectedRows;
	}

	return count;
}


std::size_t StatementImpl::executeWithoutLimit()
{
	poco_assert (_state != ST_DONE);
	std::size_t count = 0;

	do
	{
		bind();
		while (hasNext()) count += next();
	} while (canBind());

	int affectedRows = affectedRowCount();
	if (count == 0)
	{
		if (affectedRows > 0)
			return affectedRows;
	}

	return count;
}


void StatementImpl::compile()
{
	if (_state == ST_INITIALIZED || 
		_state == ST_RESET || 
		_state == ST_BOUND)
	{
		compileImpl();
		_state = ST_COMPILED;

		if (!extractions().size() && !isStoredProcedure())
		{
			std::size_t cols = columnsReturned();
			if (cols) makeExtractors(cols);
		}

		fixupExtraction();
		fixupBinding();
	}
}


void StatementImpl::bind()
{
	if (_state == ST_COMPILED)
	{
		bindImpl();
		_state = ST_BOUND;
	}
	else if (_state == ST_BOUND)
	{
		if (!hasNext())
		{
			if (canBind()) bindImpl();
			else _state = ST_DONE;
		}
	}
}


void StatementImpl::reset()
{
	resetBinding();
	resetExtraction();
	_state = ST_RESET;
}


void StatementImpl::setExtractionLimit(const Limit& extrLimit)
{
	if (!extrLimit.isLowerLimit())
		_extrLimit = extrLimit;
	else
		_lowerLimit = extrLimit.value();
}


void StatementImpl::setBulkExtraction(const Bulk& b)
{
	Limit::SizeT limit = getExtractionLimit();
	if (Limit::LIMIT_UNLIMITED != limit && b.size() != limit)
		throw InvalidArgumentException("Can not set limit for statement.");

	setExtractionLimit(b.limit());
	_bulkExtraction = BULK_EXTRACTION;
}


void StatementImpl::fixupExtraction()
{
	CountVec::iterator sIt  = _subTotalRowCount.begin();
	CountVec::iterator sEnd = _subTotalRowCount.end();
	for (; sIt != sEnd; ++sIt) *sIt = 0;

	if (_curDataSet >= _columnsExtracted.size())
	{
		_columnsExtracted.resize(_curDataSet + 1, 0);
		_subTotalRowCount.resize(_curDataSet + 1, 0);
	}

	Poco::Data::AbstractExtractionVec::iterator it    = extractions().begin();
	Poco::Data::AbstractExtractionVec::iterator itEnd = extractions().end();
	for (; it != itEnd; ++it)
	{
		(*it)->setExtractor(extractor());
		(*it)->setLimit(_extrLimit.value()),
		_columnsExtracted[_curDataSet] += (int)(*it)->numOfColumnsHandled();
	}
}


void StatementImpl::fixupBinding()
{
	// no need to call binder().reset(); here will be called before each bind anyway
	AbstractBindingVec::iterator it    = bindings().begin();
	AbstractBindingVec::iterator itEnd = bindings().end();
	for (; it != itEnd; ++it) (*it)->setBinder(binder());
}


void StatementImpl::resetBinding()
{
	AbstractBindingVec::iterator it    = bindings().begin();
	AbstractBindingVec::iterator itEnd = bindings().end();
	for (; it != itEnd; ++it) (*it)->reset();
}


void StatementImpl::resetExtraction()
{
	Poco::Data::AbstractExtractionVec::iterator it = extractions().begin();
	Poco::Data::AbstractExtractionVec::iterator itEnd = extractions().end();
	for (; it != itEnd; ++it) (*it)->reset();

	poco_assert (_curDataSet < _columnsExtracted.size());
	_columnsExtracted[_curDataSet] = 0;
}


void StatementImpl::setStorage(const std::string& storage)
{
	if (0 == icompare(DEQUE, storage))
		_storage = STORAGE_DEQUE_IMPL;
	else if (0 == icompare(VECTOR, storage))
		_storage = STORAGE_VECTOR_IMPL; 
	else if (0 == icompare(LIST, storage))
		_storage = STORAGE_LIST_IMPL;
	else if (0 == icompare(UNKNOWN, storage))
		_storage = STORAGE_UNKNOWN_IMPL;
	else
		throw NotFoundException();
}


void StatementImpl::makeExtractors(std::size_t count)
{
	for (int i = 0; i < count; ++i)
	{
		const MetaColumn& mc = metaColumn(i);
		switch (mc.type())
		{
			case MetaColumn::FDT_BOOL:
				addInternalExtract<bool>(mc); break;
			case MetaColumn::FDT_INT8:  
				addInternalExtract<Int8>(mc); break;
			case MetaColumn::FDT_UINT8:  
				addInternalExtract<UInt8>(mc); break;
			case MetaColumn::FDT_INT16:  
				addInternalExtract<Int16>(mc); break;
			case MetaColumn::FDT_UINT16: 
				addInternalExtract<UInt16>(mc); break;
			case MetaColumn::FDT_INT32:  
				addInternalExtract<Int32>(mc); break;
			case MetaColumn::FDT_UINT32: 
				addInternalExtract<UInt32>(mc); break;
			case MetaColumn::FDT_INT64:  
				addInternalExtract<Int64>(mc); break;
			case MetaColumn::FDT_UINT64: 
				addInternalExtract<UInt64>(mc); break;
			case MetaColumn::FDT_FLOAT:  
				addInternalExtract<float>(mc); break;
			case MetaColumn::FDT_DOUBLE: 
				addInternalExtract<double>(mc); break;
			case MetaColumn::FDT_STRING: 
				addInternalExtract<std::string>(mc); break;
			case MetaColumn::FDT_WSTRING:
				addInternalExtract<Poco::UTF16String>(mc); break;
			case MetaColumn::FDT_BLOB:   
				addInternalExtract<BLOB>(mc); break;
			case MetaColumn::FDT_DATE:
				addInternalExtract<Date>(mc); break;
			case MetaColumn::FDT_TIME:
				addInternalExtract<Time>(mc); break;
			case MetaColumn::FDT_TIMESTAMP:
				addInternalExtract<DateTime>(mc); break;
			default:
				throw Poco::InvalidArgumentException("Data type not supported.");
		}
	}
}


const MetaColumn& StatementImpl::metaColumn(const std::string& name) const
{
	std::size_t cols = columnsReturned();
	for (std::size_t i = 0; i < cols; ++i)
	{
		const MetaColumn& column = metaColumn(i);
		if (0 == icompare(column.name(), name)) return column;
	}

	throw NotFoundException(format("Invalid column name: %s", name));
}


std::size_t StatementImpl::activateNextDataSet()
{
	if (_curDataSet + 1 < dataSetCount())
		return ++_curDataSet;
	else
		throw NoDataException("End of data sets reached.");
}


std::size_t StatementImpl::activatePreviousDataSet()
{
	if (_curDataSet > 0)
		return --_curDataSet;
	else
		throw NoDataException("Beginning of data sets reached.");
}


void StatementImpl::addExtract(AbstractExtraction::Ptr pExtraction)
{
	poco_check_ptr (pExtraction);
	std::size_t pos = pExtraction->position();
	if (pos >= _extractors.size()) 
		_extractors.resize(pos + 1);

	pExtraction->setEmptyStringIsNull(
		_rSession.getFeature("emptyStringIsNull"));

	pExtraction->setForceEmptyString(
		_rSession.getFeature("forceEmptyString"));

	_extractors[pos].push_back(pExtraction);
}


void StatementImpl::removeBind(const std::string& name)
{
	bool found = false;

	AbstractBindingVec::iterator it = _bindings.begin();
	for (; it != _bindings.end();)
	{
		if ((*it)->name() == name) 
		{
			it = _bindings.erase(it);
			found = true;
		}
		else ++it;
	}

	if (!found)
		throw NotFoundException(name);
}


std::size_t StatementImpl::columnsExtracted(int dataSet) const
{
	if (USE_CURRENT_DATA_SET == dataSet) dataSet = static_cast<int>(_curDataSet);
	if (_columnsExtracted.size() > 0)
	{
		poco_assert (dataSet >= 0 && dataSet < _columnsExtracted.size());
		return _columnsExtracted[dataSet];
	}

	return 0;
}


std::size_t StatementImpl::rowsExtracted(int dataSet) const
{
	if (USE_CURRENT_DATA_SET == dataSet) dataSet = static_cast<int>(_curDataSet);
	if (extractions().size() > 0)
	{
		poco_assert (dataSet >= 0 && dataSet < _extractors.size());
		if (_extractors[dataSet].size() > 0)
			return _extractors[dataSet][0]->numOfRowsHandled();
	}
	
	return 0;
}


std::size_t StatementImpl::subTotalRowCount(int dataSet) const
{
	if (USE_CURRENT_DATA_SET == dataSet) dataSet = static_cast<int>(_curDataSet);
	if (_subTotalRowCount.size() > 0)
	{
		poco_assert (dataSet >= 0 && dataSet < _subTotalRowCount.size());
		return _subTotalRowCount[dataSet];
	}
	
	return 0;
}


void StatementImpl::formatSQL(std::vector<Any>& arguments)
{
	std::string sql;
	Poco::format(sql, _ostr.str(), arguments);
	_ostr.str("");
	_ostr << sql;
}


} } // namespace Poco::Data
