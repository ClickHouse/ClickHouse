//
// Preparator.cpp
//
// $Id: //poco/Main/Data/testsuite/src/Preparator.cpp#3 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Preparator.h"
#include "Poco/Data/LOB.h"
#include "Poco/Exception.h"


namespace Poco {
namespace Data {
namespace Test {


Preparator::Preparator()
{
}


Preparator::~Preparator()
{
}


void Preparator::prepare(std::size_t pos, const Poco::Int8&)
{
}


void Preparator::prepare(std::size_t pos, const Poco::UInt8&)
{
}


void Preparator::prepare(std::size_t pos, const Poco::Int16&)
{
}


void Preparator::prepare(std::size_t pos, const Poco::UInt16&)
{
}


void Preparator::prepare(std::size_t pos, const Poco::Int32&)
{
}


void Preparator::prepare(std::size_t pos, const Poco::UInt32&)
{
}


void Preparator::prepare(std::size_t pos, const Poco::Int64&)
{
}


void Preparator::prepare(std::size_t pos, const Poco::UInt64&)
{
}


#ifndef POCO_LONG_IS_64_BIT
void Preparator::prepare(std::size_t pos, const long&)
{
}


void Preparator::prepare(std::size_t pos, const unsigned long&)
{
}
#endif


void Preparator::prepare(std::size_t pos, const bool&)
{
}


void Preparator::prepare(std::size_t pos, const float&)
{
}


void Preparator::prepare(std::size_t pos, const double&)
{
}


void Preparator::prepare(std::size_t pos, const char&)
{
}


void Preparator::prepare(std::size_t pos, const std::string&)
{
}


void Preparator::prepare(std::size_t pos, const Poco::UTF16String&)
{
}


void Preparator::prepare(std::size_t pos, const Poco::Data::BLOB&)
{
}


void Preparator::prepare(std::size_t pos, const Poco::Data::CLOB&)
{
}


void Preparator::prepare(std::size_t pos, const Poco::Data::Date&)
{
}


void Preparator::prepare(std::size_t pos, const Poco::Data::Time&)
{
}


void Preparator::prepare(std::size_t pos, const Poco::DateTime&)
{
}


void Preparator::prepare(std::size_t pos, const Poco::Any&)
{
}


void Preparator::prepare(std::size_t pos, const Poco::Dynamic::Var&)
{
}


} } } // namespace Poco::Data::Test
