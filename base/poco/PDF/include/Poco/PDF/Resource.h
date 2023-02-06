//
// Resource.h
//
// Library: PDF
// Package: PDFCore
// Module:  Resource
//
// Definition of the Resource class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PDF_Resource_INCLUDED
#define PDF_Resource_INCLUDED


#include "Poco/PDF/PDF.h"
#include <vector>


namespace Poco {
namespace PDF {


template <typename R>
class Resource
	/// A Resource represents a PDF resource resource.
{
public:
	typedef R Type;

	Resource(HPDF_Doc* pPDF, const R& resource, const std::string& name = ""): 
		_pPDF(pPDF),
		_resource(resource),
		_name(name)
		/// Creates the resource
	{
	}

	Resource(const Resource& other): 
		_pPDF(other._pPDF),
		_resource(other._resource),
		_name(other._name)
		/// Copy creates the resource.
	{
	}

	virtual ~Resource()
		/// Destroys the resource.
	{
	}

	Resource& operator = (const Resource& resource)
		/// Assignment operator.
	{
		Resource tmp(resource);
		swap(tmp);
		return *this;
	}

	operator const Type& () const
		/// Const conversion operator into reference to native type.
	{
		return _resource;
	}

	bool operator == (const Resource& other) const
		/// Equality operator.
	{
		return _pPDF == other._pPDF && _resource == other._resource;
	}

	void swap(Resource& other)
	{
		using std::swap;
		
		swap(_pPDF, other._pPDF);
		swap(_resource, other._resource);
		swap(_name, other._name);
	}

	virtual const std::string& name() const
	{
		return _name;
	}

protected:
	const R& handle() const
	{
		return _resource;
	}

private:
	Resource();

	HPDF_Doc*   _pPDF;
	R           _resource;
	std::string _name;
};


//
// typedefs
//

//typedef Resource<HPDF_Annotation>  Annotation;
typedef Resource<HPDF_ExtGState>   ExtGraphicsState;

typedef HPDF_TransMatrix         TransMatrix;
typedef HPDF_Rect                Rectangle;
typedef HPDF_Point               Point;
typedef HPDF_LineCap             LineCap;
typedef HPDF_LineJoin            LineJoin;
typedef HPDF_DashMode            DashMode;
typedef HPDF_RGBColor            RGBColor;
typedef HPDF_CMYKColor           CMYKColor;
typedef std::vector<HPDF_UINT16> PatternVec;
typedef HPDF_TextWidth           TextWidth;

} } // namespace Poco::PDF


#endif // PDF_Resource_INCLUDED
