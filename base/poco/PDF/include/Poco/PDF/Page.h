//
// Page.h
//
// Library: PDF
// Package: PDFCore
// Module:  Page
//
// Definition of the Page class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PDF_Page_INCLUDED
#define PDF_Page_INCLUDED


#include "Poco/PDF/PDF.h"
#include "Poco/PDF/Resource.h"
#include "Poco/PDF/Font.h"
#include "Poco/PDF/Image.h"
#include "Poco/PDF/Encoder.h"
#include "Poco/PDF/Destination.h"
#include "Poco/PDF/TextAnnotation.h"
#include "Poco/PDF/LinkAnnotation.h"
#include "Poco/Exception.h"
#include <map>


namespace Poco {
namespace PDF {


class Document;


class PDF_API Page
	/// A Page represents a PDF page object.
{
public:
	typedef HPDF_Page                             Type;
	typedef std::map<std::string, Destination>    DestinationContainer;
	typedef std::map<std::string, TextAnnotation> TextAnnotationContainer;
	typedef std::map<std::string, LinkAnnotation> LinkAnnotationContainer;

	enum Size
	{
		PAGE_SIZE_LETTER = HPDF_PAGE_SIZE_LETTER, 
			/// 8½ x 11 (Inches), 612 x 792 px
		PAGE_SIZE_LEGAL = HPDF_PAGE_SIZE_LEGAL,
			/// 8½ x 14 (Inches), 612 x 1008 px
		PAGE_SIZE_A3 = HPDF_PAGE_SIZE_A3,
			/// 297 × 420 (mm), 841.89 x 1199.551 px
		PAGE_SIZE_A4 = HPDF_PAGE_SIZE_A4,
			/// 210 × 297 (mm), 595.276 x 841.89 px
		PAGE_SIZE_A5 = HPDF_PAGE_SIZE_A5,
			/// 148 × 210 (mm), 419.528 x 595.276 px
		PAGE_SIZE_B4 = HPDF_PAGE_SIZE_B4,
			/// 250 × 353 (mm), 708.661 x 1000.63 px
		PAGE_SIZE_B5 = HPDF_PAGE_SIZE_B5,
			/// 176 × 250 (mm), 498.898 x 708.661 px
		PAGE_SIZE_EXECUTIVE = HPDF_PAGE_SIZE_EXECUTIVE,
			/// 7½ x 10½ (Inches), 522 x 756 px
		PAGE_SIZE_US4x6 = HPDF_PAGE_SIZE_US4x6,
			/// 4 x 6 (Inches), 288 x 432 px
		PAGE_SIZE_US4x8 = HPDF_PAGE_SIZE_US4x8,
			/// 4 x 8 (Inches), 288 x 576 px
		PAGE_SIZE_US5x7 = HPDF_PAGE_SIZE_US5x7,
			/// 5 x 7 (Inches), 360 x 504 px
		PAGE_SIZE_COMM10 = HPDF_PAGE_SIZE_COMM10
			/// 4.125 x 9.5 (Inches) 297x 684 px
	};

	enum Orientation
	{
		ORIENTATION_PORTRAIT = HPDF_PAGE_PORTRAIT,
			// Portrait orientation.
		ORIENTATION_LANDSCAPE = HPDF_PAGE_LANDSCAPE
			// Landscape orientation.
	};

	enum RenderMode
	{
		RENDER_FILL = HPDF_FILL,
		RENDER_STROKE = HPDF_STROKE,
		RENDER_FILL_THEN_STROKE = HPDF_FILL_THEN_STROKE,
		RENDER_INVISIBLE = HPDF_INVISIBLE,
		RENDER_FILL_CLIPPING = HPDF_FILL_CLIPPING,
		RENDER_STROKE_CLIPPING = HPDF_STROKE_CLIPPING,
		RENDER_FILL_STROKE_CLIPPING = HPDF_FILL_STROKE_CLIPPING,
		RENDER_CLIPPING = HPDF_CLIPPING,
		RENDER_RENDERING_MODE_EOF = HPDF_RENDERING_MODE_EOF
	};

	enum ColorSpace
	{
		CS_DEVICE_GRAY = HPDF_CS_DEVICE_GRAY,
		CS_DEVICE_RGB = HPDF_CS_DEVICE_RGB,
		CS_DEVICE_CMYK = HPDF_CS_DEVICE_CMYK,
		CS_CAL_GRAY = HPDF_CS_CAL_GRAY,
		CS_CAL_RGB = HPDF_CS_CAL_RGB,
		CS_LAB = HPDF_CS_LAB,
		CS_ICC_BASED = HPDF_CS_ICC_BASED,
		CS_SEPARATION = HPDF_CS_SEPARATION,
		CS_DEVICE_N = HPDF_CS_DEVICE_N,
		CS_INDEXED = HPDF_CS_INDEXED,
		CS_PATTERN = HPDF_CS_PATTERN,
		CS_EOF = HPDF_CS_EOF
	};

	enum TransitionStyle
	{
		TS_WIPE_RIGHT = HPDF_TS_WIPE_RIGHT,
		TS_WIPE_UP = HPDF_TS_WIPE_UP,
		TS_WIPE_LEFT = HPDF_TS_WIPE_LEFT,
		TS_WIPE_DOWN = HPDF_TS_WIPE_DOWN,
		TS_BARN_DOORS_HORIZONTAL_OUT = HPDF_TS_BARN_DOORS_HORIZONTAL_OUT,
		TS_BARN_DOORS_HORIZONTAL_IN = HPDF_TS_BARN_DOORS_HORIZONTAL_IN,
		TS_BARN_DOORS_VERTICAL_OUT = HPDF_TS_BARN_DOORS_VERTICAL_OUT,
		TS_BARN_DOORS_VERTICAL_IN = HPDF_TS_BARN_DOORS_VERTICAL_IN,
		TS_BOX_OUT = HPDF_TS_BOX_OUT,
		TS_BOX_IN = HPDF_TS_BOX_IN,
		TS_BLINDS_HORIZONTAL = HPDF_TS_BLINDS_HORIZONTAL,
		TS_BLINDS_VERTICAL = HPDF_TS_BLINDS_VERTICAL,
		TS_DISSOLVE = HPDF_TS_DISSOLVE,
		TS_GLITTER_RIGHT = HPDF_TS_GLITTER_RIGHT,
		TS_GLITTER_DOWN = HPDF_TS_GLITTER_DOWN,
		TS_GLITTER_TOP_LEFT_TO_BOTTOM_RIGHT = HPDF_TS_GLITTER_TOP_LEFT_TO_BOTTOM_RIGHT,
		TS_REPLACE = HPDF_TS_REPLACE
	};

	enum TextAlignment
	{
		TEXT_ALIGN_LEFT = HPDF_TALIGN_LEFT,
			/// The text is aligned to left.
		TEXT_ALIGN_RIGHT = HPDF_TALIGN_RIGHT,
			/// The text is aligned to right.
		TEXT_ALIGN_CENTER = HPDF_TALIGN_CENTER,
			/// The text is aligned to center.
		TEXT_ALIGN_JUSTIFY = HPDF_TALIGN_JUSTIFY
			/// Add spaces between the words to justify both left and right side.
	};

	Page(Document* pDocument,
		const HPDF_Page& page,
		Size pageSize = PAGE_SIZE_LETTER,
		Orientation orientation = ORIENTATION_PORTRAIT);
		/// Creates the page

	Page(const Page& other);
		/// Copy creates the page.

	virtual ~Page();
		/// Destroys the page.

	Page& operator = (const Page& page);
		/// Assignment operator.

	operator const Type& () const;
		/// Const conversion operator into reference to native type.

	bool operator == (const Page& other) const;
		/// Equality operator.

	void swap(Page& other);
		/// Swaps this page with another.

	void setWidth(float value);
		/// Sets the page width.

	float getWidth() const;
		/// Gets the page width.

	void setHeight(float value);
		/// Sets the page height.

	float getHeight() const;
		/// Gets the page height.

	void setSize(Size size);
		/// Sets the page size.

	Size getSize() const;
		/// Returns the page size.

	void setOrientation(Orientation orientation);
		/// Sets the page orientation.

	Orientation getOrientation() const;
		/// Returns the page orientation.

	void setSizeAndOrientation(Size size, Orientation orientation);
		/// Sets the page size and orientation.

	void setRotation(int angle);
		/// Sets the rotation for the page.
		/// Angle must be multiple of 90.

	void setFont(const Font& font, float size);
		/// Sets the font.

	void setFont(const std::string& fontName, float size, const std::string& encoding = "");
		/// Sets the font. The name must be a valid Base14 PDF internal font.

	void setTTFont(const std::string& name, float size, const std::string& encoding = "UTF-8", bool embed = true);
		/// Sets the external true type font. Name must be a valid path to .ttf file.
		/// If embed is tru, font will be embedded int othe document.

	float textWidth(const std::string& text);
		/// Returns the width of the supplied text.

	void  beginText();
		/// Begins a text object and sets the current text position to the point (0,0).

	void endText();
		/// Ends a text object.

	void write(float xPos, float yPos, const std::string& text);
		/// Write text at specified position.

	void writeOnce(float xPos, float yPos, const std::string& text);
		/// Begins, writes and ends text object.

	void write(const std::string& text);
		/// Writes the text at the current position on the page.

	void writeNextLine(const std::string& text);
		/// Moves the current text position to the start of the next line and
		/// prints the text at the current position on the page.
	
	void writeNextLineEx(float wordSpace, float charSpace, const std::string& text);
		/// Moves the current text position to the start of the next line, sets the word spacing, 
		/// character spacing and prints the text at the current position on the page.

	int writeOnceInRectangle(float left,
		float top,
		float right,
		float bottom,
		const std::string& text,
		TextAlignment align = TEXT_ALIGN_LEFT);
		/// Begins, writes and ends text objectinside the specified region. 
		/// Returns the number of characters written.

	int writeInRectangle(float left,
		float top,
		float right,
		float bottom,
		const std::string& text,
		TextAlignment align);
		/// Writes the text inside the specified region. 
		/// Returns the number of characters written.

	void drawImage(Image image, float x, float y, float width, float height);
		/// Draws an image in one operation.

	const Destination& createDestination(const std::string& name);
		/// Creates ad returns reference to destination.

	const TextAnnotation& createTextAnnotation(const std::string& name, 
		const Rectangle& rect,
		const std::string& text,
		const Encoder& encoder);
		/// Creates ad returns reference to text annotation.

	const LinkAnnotation& createLinkAnnotation(const std::string& name, 
		const Rectangle& rect,
		const Destination& dest);
		/// Creates ad returns reference to destination link annotation.

	const LinkAnnotation& createURILinkAnnotation(const std::string& name, 
		const Rectangle& rect,
		const std::string& uri);
		/// Creates ad returns reference to URI annotation.

	int getGraphicsMode() const;
		/// Returns current graphics mode.

	int getGraphicStateDepth() const;
		/// Returns current graphics mode.

	void setExtGraphicsState(ExtGraphicsState state);
		/// Sets the graphic state.

	void saveGraphics();
		/// Pushes graphics parameters on the stack.

	void restoreGraphics();
		/// Restores graphics parameters from the stack.

	void concatenate(const std::vector<float>& values);
		/// Concatenates the page's current transformation matrix and specified matrix.

	void moveTo(float x, float y);
		/// Starts a new subpath and move the current point for drawing path.
		/// Sets the start point for the path to the point (x, y).

	void lineTo(float x, float y);
		/// Appends a path from the current point to the specified point..

	void curveTo(const std::vector<float>& values);
		/// Appends a Bézier curve to the current path using two specified points.
		/// The point (x1, y1) and the point (x2, y2) are used as the control points 
		/// for a Bézier curve and current point is moved to the point (x3, y3)

	void curveToRight(float x2, float y2, float x3, float y3);
		/// Appends a Bézier curve to the right of the current point using two specified points.
		/// The current point and the point (x2, y2) are used as the control points 
		/// for a Bézier curve and current point is moved to the point (x3, y3)

	void curveToLeft(float x2, float y2, float x3, float y3);
		/// Appends a Bézier curve to the left of the current point using two specified points.
		/// The current point and the point (x2, y2) are used as the control points 
		/// for a Bézier curve and current point is moved to the point (x3, y3)

	void closePath();
		/// Appends a straight line from the current point to the start point of sub path.
		/// The current point is moved to the start point of sub path. 

	void rectangle(float x, float y, float width, float height);
		/// Draws a rectangle.

	void circle(float x, float y, float radius);
		/// Draws a circle.

	void arc(float x, float y, float radius, float beginAngle, float endAngle);
		/// Draws an arc.

	void ellipse(float x, float y, float xRadius, float yRadius);
		/// Draws an ellips.

	void stroke();
		/// Paints the current path.

	void closeAndStroke();
		/// Closes the current path and paints it.

	void fill();
		/// Fills the current path using the nonzero winding number rule.

	void EOFill();
		/// Fills the current path using the even-odd rule.

	void fillStroke();
		/// Fills the current path using the nonzero winding number rule and then paints it.

	void EOFillStroke();
		/// Fills the current path using the even-odd rule and then paints it.

	void closeFillAndStroke();
		/// Closes the current path, fills the current path using the nonzero winding number 
		/// rule and then paints it.

	void closeFillAndEOStroke();
		/// Closes the current path, fills the current path using the even-odd rule and then paints it.

	void endPath();
		/// Ends the path object without filling and painting operation.

	void clip();
		///

	void eoClip();
		///

	Point getPos() const;
		/// Returns the current position.

	Point getTextPos() const;
		/// Returns the current position for text showing.

	void moveTextPos(float x, float y);
		/// Moves the current text position to the start of the next line
		/// using specified offset values. If the start position of the current 
		/// line is (x1, y1), the start of the next line is (x1 + x, y1 + y).
	
	void moveTextNextLine(float x, float y);
		/// Moves the current text position to the start of the next line 
		/// using specified offset values, and sets the text leading to -y. 
		/// If the start position of the current line is (x1, y1), the start 
		/// of the next line is (x1 + x, y1 + y). 

	void moveTextNextLine();
		/// Moves the current text position to the start of the next line.
		/// If the start position of the current line is (x1, y1), the start of 
		/// the next line is (x1, y1 - text leading).
		///
		/// NOTE:
		/// Since the default value of Text Leading is 0,  an application has to 
		/// invoke HPDF_Page_SetTextLeading() before HPDF_Page_MoveTextPos2() to set
		/// text leading.

	const Font& getFont() const;
		/// Returns the current font.

	float getFontSize() const;
		/// Returns the current Font size.

	TransMatrix getTransMatrix() const;
		/// Returns the current transformation matrix.

	TransMatrix getTextMatrix() const;
		/// Returns the current text transformation matrix.

	float getLineWidth() const;
		/// Returns the current line width.

	void setLineWidth(float width);
		/// Sets the line width.

	LineCap getLineCap() const;
		/// Returns the current line cap.

	void setLineCap(LineCap cap) const;
		/// Sets the line cap.

	LineJoin getLineJoin() const;
		/// Returns the current line join.

	void setLineJoin(LineJoin join) const;
		/// Returns the current line join.

	float getMiterLimit() const;
		/// Returns the current miter limit.

	void setMiterLimit(float limit) const;
		/// Sets the miter limit.

	DashMode getDashMode() const;
		/// Returns current dash mode.

	void setDashMode(const PatternVec& pattern, int paramNo, int phase) const;
		/// Sets teh dash mode.

	float getFlatness() const;
		/// Returns the current flatness.

	float getCharSpace() const;
		/// Returns the current character space.

	void setCharSpace(float value);
		/// Sets the current character space.

	float getWordSpace() const;
		/// Returns the current word space.

	void setWordSpace(float value);
		/// Sets the current word space.

	float getHorizontalScale() const;
		/// Returns the current horizontal scaling.

	void setHorizontalScale(float value);
		/// Sets the current horizontal scaling.

	float getTextLead() const;
		/// Returns the current text leading.

	void setTextLead(float value);
		/// Sets the current text leading.

	RenderMode getTextRenderMode() const;
		/// Returns the current text rendering mode.

	void setTextRenderMode(RenderMode value);
		/// Sets the current text rendering mode.

	float getTextRise() const;
		/// Returns the current text leading.

	void setTextRise(float value);
		/// Sets the current text leading.

	RGBColor getRGBFill() const;
		/// Returns current RGB fill.

	void setRGBFill(RGBColor value);
		/// Sets current RGB fill.

	RGBColor getRGBStroke() const;
		/// Returns current RGB stroke.

	void setRGBStroke(RGBColor value);
		/// Sets current RGB stroke.

	CMYKColor getCMYKFill() const;
		/// Returns current CMYK fill.

	void setCMYKFill(CMYKColor value);
		/// Sets current CMYK fill.

	CMYKColor getCMYKStroke() const;
		/// Returns current CMYK stroke.

	void setCMYKStroke(CMYKColor value);
		/// Returns current CMYK stroke.

	float getGreyFill() const;
		/// Returns current grey fill.

	void setGreyFill(float value);
		/// Sets current grey fill.

	float getGreyStroke() const;
		/// Returns current grey stroke.

	void setGreyStroke(float value);
		/// Sets current grey stroke.

	ColorSpace getStrokeColorSpace() const;
		/// Returns current stroking color space.

	ColorSpace getFillColorSpace() const;
		/// Returns current filling color space.

	void setSlideShow(TransitionStyle type, float displayTime, float transitionTime);
		/// Configures the setting for slide transition of the page

private:
	Page();

	Document*                _pDocument;
	HPDF_Page                _page;
	Size                     _size;
	Orientation              _orientation;
	DestinationContainer    _destinations;
	TextAnnotationContainer _textAnnotations;
	LinkAnnotationContainer _linkAnnotations;
	mutable Font*           _pCurrentFont;
};


//
// inlines
//


inline Page::operator const Page::Type& () const
{
	return _page;
}


inline void Page::setWidth(float value)
{
	HPDF_Page_SetWidth(_page, value);
}


inline float Page::getWidth() const
{
	return HPDF_Page_GetWidth(_page);
}


inline void Page::setHeight(float value)
{
	HPDF_Page_SetHeight(_page, value);
}


inline float Page::getHeight() const
{
	return HPDF_Page_GetHeight(_page);
}


inline void Page::setSizeAndOrientation(Size size, Orientation orientation)
{
	_size = size;
	_orientation = orientation;
	HPDF_Page_SetSize(_page, static_cast<HPDF_PageSizes>(size), static_cast<HPDF_PageDirection>(orientation));
}


inline void Page::setSize(Size size)
{
	_size = size;
	HPDF_Page_SetSize(_page, static_cast<HPDF_PageSizes>(size), static_cast<HPDF_PageDirection>(_orientation));
}


inline void Page::setOrientation(Orientation orientation)
{
	_orientation = orientation;
	HPDF_Page_SetSize(_page, static_cast<HPDF_PageSizes>(_size), static_cast<HPDF_PageDirection>(orientation));
}


inline Page::Size Page::getSize() const
{
	return _size;
}


inline Page::Orientation Page::getOrientation() const
{
	return _orientation;
}


inline void Page::setFont(const Font& font, float size)
{
	HPDF_Page_SetFontAndSize(_page, font, size);
}


inline int Page::getGraphicsMode() const
{
	return HPDF_Page_GetGMode(_page);
}


inline int Page::getGraphicStateDepth() const
{
	return HPDF_Page_GetGStateDepth(_page);
}


inline void Page::setExtGraphicsState(ExtGraphicsState state)
{
	HPDF_Page_SetExtGState(_page, state);
}


inline void Page::saveGraphics()
{
	HPDF_Page_GSave(_page);
}


inline void Page::restoreGraphics()
{
	HPDF_Page_GRestore(_page);
}


inline void Page::concatenate(const std::vector<float>& values)
{
	if (values.size() < 6) 
		throw InvalidArgumentException("Needs six values");

	HPDF_Page_Concat(_page,
		values[0],
		values[1],
		values[2],
		values[3],
		values[4],
		values[5]);
}


inline void Page::moveTo(float x, float y)
{
	HPDF_Page_MoveTo(_page, x, y);
}


inline void Page::lineTo(float x, float y)
{
	HPDF_Page_LineTo(_page, x, y);
}


inline void Page::curveTo(const std::vector<float>& values)
{
	if (values.size() < 6) 
		throw InvalidArgumentException("Needs six values");

	HPDF_Page_CurveTo(_page,
		values[0],
		values[1],
		values[2],
		values[3],
		values[4],
		values[5]);
}


inline void Page::curveToRight(float x2, float y2, float x3, float y3)
{
	HPDF_Page_CurveTo2(_page, x2, y2, x3, y3);
}


inline void Page::curveToLeft(float x2, float y2, float x3, float y3)
{
	HPDF_Page_CurveTo3(_page, x2, y2, x3, y3);
}


inline void Page::closePath()
{
	HPDF_Page_ClosePath(_page);
}


inline void Page::rectangle(float x, float y, float width, float height)
{
	HPDF_Page_Rectangle(_page, x, y, width, height);
}


inline void Page::stroke()
{
	HPDF_Page_Stroke(_page);
}


inline void Page::closeAndStroke()
{
	HPDF_Page_ClosePathStroke(_page);
}


inline void Page::fill()
{
	HPDF_Page_Fill(_page);
}


inline void Page::EOFill()
{
	HPDF_Page_Eofill(_page);
}


inline void Page::fillStroke()
{
	HPDF_Page_FillStroke(_page);
}


inline void Page::EOFillStroke()
{
	HPDF_Page_EofillStroke(_page);
}


inline void Page::closeFillAndStroke()
{
	HPDF_Page_ClosePathFillStroke(_page);
}


inline void Page::closeFillAndEOStroke()
{
	HPDF_Page_ClosePathEofillStroke(_page);
}


inline void Page::endPath()
{
	HPDF_Page_EndPath(_page);
}


inline void Page::clip()
{
	HPDF_Page_Clip(_page);
}


inline void Page::eoClip()
{
	HPDF_Page_Eoclip(_page);
}


inline Point Page::getPos() const
{
	return HPDF_Page_GetCurrentPos(_page);
}


inline Point Page::getTextPos() const
{
	return HPDF_Page_GetCurrentTextPos(_page);
}


inline void Page::moveTextPos(float x, float y)
{
	HPDF_Page_MoveTextPos(_page, x, y);
}

	
inline void Page::moveTextNextLine(float x, float y)
{
	HPDF_Page_MoveTextPos2(_page, x, y);
}

	
inline void Page::moveTextNextLine()
{
	HPDF_Page_MoveToNextLine(_page);
}


inline float Page::getFontSize() const
{
	return HPDF_Page_GetCurrentFontSize(_page);
}


inline TransMatrix Page::getTransMatrix() const
{
	return HPDF_Page_GetTransMatrix(_page);
}


inline TransMatrix Page::getTextMatrix() const
{
	return HPDF_Page_GetTextMatrix(_page);
}


inline float Page::getLineWidth() const
{
	return HPDF_Page_GetLineWidth(_page);
}


inline void Page::setLineWidth(float width)
{
	HPDF_Page_SetLineWidth(_page, width);
}


inline LineCap Page::getLineCap() const
{
	return HPDF_Page_GetLineCap(_page);
}


inline void Page::setLineCap(LineCap cap) const
{
	HPDF_Page_SetLineCap(_page, cap);
}


inline LineJoin Page::getLineJoin() const
{
	return HPDF_Page_GetLineJoin(_page);
}


inline void Page::setLineJoin(LineJoin join) const
{
	HPDF_Page_SetLineJoin(_page, join);
}


inline float Page::getMiterLimit() const
{
	return HPDF_Page_GetMiterLimit(_page);
}


inline void Page::setMiterLimit(float limit) const
{
	HPDF_Page_SetMiterLimit(_page, limit);
}


inline DashMode Page::getDashMode() const
{
	return HPDF_Page_GetDash(_page);
}


inline void Page::setDashMode(const PatternVec& pattern, int paramNo, int phase) const
{
	HPDF_Page_SetDash(_page, &pattern[0], 
		static_cast<HPDF_UINT>(paramNo),
		static_cast<HPDF_UINT>(phase));
}


inline float Page::getFlatness() const
{
	return HPDF_Page_GetFlat(_page);
}


inline float Page::getCharSpace() const
{
	return HPDF_Page_GetCharSpace(_page);
}


inline void Page::setCharSpace(float value)
{
	HPDF_Page_SetCharSpace(_page, value);
}


inline float Page::getWordSpace() const
{
	return HPDF_Page_GetWordSpace(_page);
}


inline void Page::setWordSpace(float value)
{
	HPDF_Page_SetWordSpace(_page, value);
}


inline float Page::getHorizontalScale() const
{
	return HPDF_Page_GetHorizontalScalling(_page);
}


inline void Page::setHorizontalScale(float value)
{
	HPDF_Page_SetHorizontalScalling(_page, value);
}


inline float Page::getTextLead() const
{
	return HPDF_Page_GetTextLeading(_page);
}


inline void Page::setTextLead(float value)
{
	HPDF_Page_SetTextLeading(_page, value);
}


inline Page::RenderMode Page::getTextRenderMode() const
{
	return static_cast<RenderMode>(HPDF_Page_GetTextRenderingMode(_page));
}


inline void Page::setTextRenderMode(RenderMode value)
{
	HPDF_Page_SetTextRenderingMode(_page, static_cast<HPDF_TextRenderingMode>(value));
}


inline float Page::getTextRise() const
{
	return HPDF_Page_GetTextRise(_page);
}


inline void Page::setTextRise(float value)
{
	HPDF_Page_SetTextRise(_page, value);
}


inline RGBColor Page::getRGBFill() const
{
	return HPDF_Page_GetRGBFill(_page);
}


inline void Page::setRGBFill(RGBColor value)
{
	HPDF_Page_SetRGBFill(_page, value.r, value.g, value.b);
}


inline RGBColor Page::getRGBStroke() const
{
	return HPDF_Page_GetRGBStroke(_page);
}


inline void Page::setRGBStroke(RGBColor value)
{
	HPDF_Page_SetRGBStroke(_page, value.r, value.g, value.b);
}


inline CMYKColor Page::getCMYKFill() const
{
	return HPDF_Page_GetCMYKFill(_page);
}


inline void Page::setCMYKFill(CMYKColor value)
{
	HPDF_Page_SetCMYKFill(_page, value.c, value.m, value.y, value.k);
}


inline CMYKColor Page::getCMYKStroke() const
{
	return HPDF_Page_GetCMYKStroke(_page);
}


inline void Page::setCMYKStroke(CMYKColor value)
{
	HPDF_Page_SetCMYKStroke(_page, value.c, value.m, value.y, value.k);
}


inline float Page::getGreyFill() const
{
	return HPDF_Page_GetGrayFill(_page);
}


inline void Page::setGreyFill(float value)
{
	HPDF_Page_SetGrayFill(_page, value);
}


inline float Page::getGreyStroke() const
{
	return HPDF_Page_GetGrayStroke(_page);
}


inline void Page::setGreyStroke(float value)
{
	HPDF_Page_SetGrayStroke(_page, value);
}


inline Page::ColorSpace Page::getStrokeColorSpace() const
{
	return static_cast<ColorSpace>(HPDF_Page_GetStrokingColorSpace(_page));
}


inline Page::ColorSpace Page::getFillColorSpace() const
{
	return static_cast<ColorSpace>(HPDF_Page_GetFillingColorSpace(_page));
}


inline void Page::setSlideShow(TransitionStyle type, float displayTime, float transitionTime)
{
	HPDF_Page_SetSlideShow(_page,
		static_cast<HPDF_TransitionStyle>(type),
		displayTime,
		transitionTime);
}


inline void Page::beginText()
{
	HPDF_Page_BeginText(_page);
}

inline void Page::endText()
{
	HPDF_Page_EndText(_page);
}


inline void Page::write(float xPos, float yPos, const std::string& text)
{
	HPDF_Page_TextOut(_page, xPos, yPos, text.c_str());
}


inline void Page::write(const std::string& text)
{
	HPDF_Page_ShowText(_page, text.c_str());
}


inline void Page::writeNextLine(const std::string& text)
{
	HPDF_Page_ShowTextNextLine(_page, text.c_str());
}


inline void Page::writeNextLineEx(float wordSpace, float charSpace, const std::string& text)
{
	HPDF_Page_ShowTextNextLineEx(_page, wordSpace, charSpace, text.c_str());
}


inline int Page::writeInRectangle(float left,
	float top,
	float right,
	float bottom,
	const std::string& text,
	TextAlignment align)
{
	HPDF_UINT ret = 0;
	HPDF_Page_TextRect(_page,
		left,
		top,
		right,
		bottom,
		text.c_str(),
		static_cast<HPDF_TextAlignment>(align),
		&ret);

	return static_cast<int>(ret);
}


inline void Page::drawImage(Image image, float x, float y, float width, float height)
{
	HPDF_Page_DrawImage(_page, image, x, y, width, height);
}


inline void Page::circle(float x, float y, float radius)
{
	HPDF_Page_Circle(_page, x, y, radius);
}


inline void Page::arc(float x, float y, float radius, float beginAngle, float endAngle)
{
	HPDF_Page_Arc(_page, x, y, radius, beginAngle, endAngle);
}


inline void Page::ellipse(float x, float y, float xRadius, float yRadius)
{
	HPDF_Page_Ellipse(_page, x, y, xRadius, yRadius);
}


} } // namespace Poco::PDF


#endif // PDF_Page_INCLUDED
