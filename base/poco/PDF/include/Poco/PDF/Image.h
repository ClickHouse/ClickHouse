//
// Image.h
//
// Library: PDF
// Package: PDFCore
// Module:  Image
//
// Definition of the Image class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PDF_Image_INCLUDED
#define PDF_Image_INCLUDED


#include "Poco/PDF/PDF.h"
#include "Poco/PDF/Resource.h"


namespace Poco {
namespace PDF {


class PDF_API Image: public Resource<HPDF_Image>
	/// Image class represents image resource.
{
public:
	Image(HPDF_Doc* pPDF, const HPDF_Image& resource, const std::string& name = "");
		/// Creates the image.

	~Image();
		/// Destroys the image.

	Point size() const;
		/// Returns the size of the image.
	float width() const;
		/// Returns the width of the image.

	float height() const;
		/// Returns the height of the image.

	Poco::UInt32 bitsPerColor() const;
		/// Returns the number of bits per color.

	std::string colorSpace() const;
		/// Returns the name of the image's color space.

	void colorMask(Poco::UInt32 redMin,
		Poco::UInt32 redMax,
		Poco::UInt32 greenMin,
		Poco::UInt32 greenMax,
		Poco::UInt32 blueMin,
		Poco::UInt32 blueMax);
		/// Sets the transparent color of the image by the RGB range values.
		/// The color within the range is displayed as a transparent color.
		/// The Image must be of the RGB color space.
};


//
// inlines
//

inline Point Image::size() const
{
	return HPDF_Image_GetSize(handle());
}


inline float Image::width() const
{
	return static_cast<float>(HPDF_Image_GetWidth(handle()));
}


inline float Image::height() const
{
	return static_cast<float>(HPDF_Image_GetHeight(handle()));
}


inline Poco::UInt32 Image::bitsPerColor() const
{
	return HPDF_Image_GetBitsPerComponent(handle());
}


inline std::string Image::colorSpace() const
{
	return HPDF_Image_GetColorSpace(handle());
}


inline void Image::colorMask(Poco::UInt32 redMin,
	Poco::UInt32 redMax,
	Poco::UInt32 greenMin,
	Poco::UInt32 greenMax,
	Poco::UInt32 blueMin,
	Poco::UInt32 blueMax)
{
	HPDF_Image_SetColorMask(handle(),
		redMin,
		redMax,
		greenMin,
		greenMax,
		blueMin,
		blueMax);
}


} } // namespace Poco::PDF


#endif // PDF_Image_INCLUDED
