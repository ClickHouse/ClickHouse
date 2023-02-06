//
// Encoder.h
//
// Library: PDF
// Package: PDFCore
// Module:  Encoder
//
// Definition of the Encoder class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PDF_Encoder_INCLUDED
#define PDF_Encoder_INCLUDED


#include "Poco/PDF/PDF.h"
#include "Poco/PDF/Resource.h"
#include "Poco/Exception.h"


namespace Poco {
namespace PDF {


class PDF_API Encoder: public Resource<HPDF_Encoder>
	/// Encoder class represents encoder resource.
{
public:
	enum Type
	{		
		ENCODER_TYPE_SINGLE_BYTE = HPDF_ENCODER_TYPE_SINGLE_BYTE,
			/// This encoder is an encoder for single byte characters.
		ENCODER_TYPE_DOUBLE_BYTE = HPDF_ENCODER_TYPE_DOUBLE_BYTE,
			/// This encoder is an encoder for multi byte characters.
		ENCODER_TYPE_UNINITIALIZED = HPDF_ENCODER_TYPE_UNINITIALIZED,
			/// This encoder is uninitialized. (May be it is an encoder for multi byte characters.)
		ENCODER_TYPE_UNKNOWN = HPDF_ENCODER_UNKNOWN
			/// Invalid encoder.
	};

	enum ByteType
	{
		ENCODER_BYTE_TYPE_SINGLE = HPDF_BYTE_TYPE_SINGLE,
			/// Single byte character.
		ENCODER_BYTE_TYPE_LEAD = HPDF_BYTE_TYPE_LEAD,
			/// Lead byte of a double-byte character.
		ENCODER_BYTE_TYPE_TRIAL = HPDF_BYTE_TYPE_TRIAL,
			/// Trailing byte of a double-byte character.
		ENCODER_BYTE_TYPE_UNKNOWN = HPDF_BYTE_TYPE_UNKNOWN
			/// Invalid encoder or cannot judge the byte type.
	};

	enum WriteMode
	{
		WRITE_MODE_HORIZONTAL = HPDF_WMODE_HORIZONTAL,
			/// Horizontal writing mode.
		WRITE_MODE_VERTICAL = HPDF_WMODE_VERTICAL
			/// Vertical writing mode;
	};

	Encoder(HPDF_Doc* pPDF, const HPDF_Encoder& resource, const std::string& name = "");
		/// Creates the encoder.

	~Encoder();
		/// Destroys the encoder.

	Type getType() const;
		/// Returns the type of an encoding object.

	ByteType getByteType(const std::string& text, int index = 0) const;
		/// Returns the type of byte in the text at position index 

	WriteMode writeMode();
		/// Returns the writing mode for the encoding object. 
};


//
// inlines
//

inline Encoder::Type Encoder::getType() const
{
	return static_cast<Type>(HPDF_Encoder_GetType(handle()));
}


inline Encoder::ByteType Encoder::getByteType(const std::string& text, int index) const
{
	if (index < 0)
		throw InvalidArgumentException("Negative values not allowed.");

	return static_cast<ByteType>(HPDF_Encoder_GetByteType(handle(), 
		text.c_str(),
		static_cast<HPDF_UINT>(index)));
}


inline Encoder::WriteMode Encoder::writeMode()
{
	return static_cast<WriteMode>(HPDF_Encoder_GetWritingMode(handle()));
}


} } // namespace Poco::PDF


#endif // PDF_Encoder_INCLUDED
