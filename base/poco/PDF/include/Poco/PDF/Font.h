//
// Font.h
//
// Library: PDF
// Package: PDFCore
// Module:  Font
//
// Definition of the Font class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PDF_Font_INCLUDED
#define PDF_Font_INCLUDED


#include "Poco/PDF/PDF.h"
#include "Poco/PDF/Resource.h"


namespace Poco {
namespace PDF {


class PDF_API Font: public Resource<HPDF_Font>
	/// Font class represents font resource.
{
public:
	Font(HPDF_Doc* pPDF, const HPDF_Font& resource);
		/// Creates the font.

	~Font();
		/// Destroys the font.

	std::string encodingName() const;
		/// Returns the name of the encoding.

	int unicodeWidth(Poco::UInt16 ch) const;
		/// Returns the screen width of 16-bit Unicode character.

	Rectangle boundingBox() const;
		/// Returns the font's bounding box.

	int ascent() const;
		/// Returns the vertical ascent of the font.

	int descent() const;
		/// Returns the vertical ascent of the font.

	int lowerHeight() const;
		/// Returns the distance from the baseline of lowercase letters.

	int upperHeight() const;
		/// Returns the distance from the baseline of uppercase letters.

	TextWidth textWidth(const std::string& text);
		/// Returns total width of the text, number of characters and number of the words.

	int measureText(const std::string& text,
		float width,
		float fontSize,
		float charSpace,
		float wordSpace,
		bool wordWrap);
		/// Calculates the byte length which can be included within the specified width.
};


//
// inlines
//

inline std::string Font::encodingName() const
{
	return HPDF_Font_GetEncodingName(handle());
}


inline int Font::unicodeWidth(Poco::UInt16 ch) const
{
	return HPDF_Font_GetUnicodeWidth(handle(), ch);
}


inline Rectangle Font::boundingBox() const
{
	return HPDF_Font_GetBBox(handle());
}


inline int Font::ascent() const
{
	return HPDF_Font_GetAscent(handle());
}


inline int Font::descent() const
{
	return HPDF_Font_GetDescent(handle());
}


inline int Font::lowerHeight() const
{
	return static_cast<int>(HPDF_Font_GetXHeight(handle()));
}


inline int Font::upperHeight() const
{
	return static_cast<int>(HPDF_Font_GetCapHeight(handle()));
}


inline TextWidth Font::textWidth(const std::string& text)
{
	return HPDF_Font_TextWidth(handle(), 
		reinterpret_cast<const HPDF_BYTE*>(text.data()), 
		static_cast<HPDF_UINT>(text.size()));
}


inline int Font::measureText(const std::string& text,
	float width,
	float fontSize,
	float charSpace,
	float wordSpace,
	bool wordWrap)
{
	return static_cast<int>(HPDF_Font_MeasureText(handle(),
		reinterpret_cast<const HPDF_BYTE*>(text.data()),
		static_cast<HPDF_UINT>(text.size()),
		width,
		fontSize,
		charSpace,
		wordSpace,
		wordWrap,
		0));
}


} } // namespace Poco::PDF


#endif // PDF_Font_INCLUDED
