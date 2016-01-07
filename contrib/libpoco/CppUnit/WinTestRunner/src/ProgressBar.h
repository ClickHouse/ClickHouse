//
// ProgressBar.h
//
// $Id: //poco/1.4/CppUnit/WinTestRunner/src/ProgressBar.h#1 $
//


#ifndef ProgressBar_INCLUDED
#define ProgressBar_INCLUDED


#include "CppUnit/CppUnit.h"
#include <afxwin.h>


namespace CppUnit {


/* A Simple ProgressBar for test execution display
 */
class ProgressBar
{
public:
	ProgressBar(CWnd* baseWindow, CRect& bounds);

	void step(bool successful);
	void paint(CDC& dc);
	int scale(int value);
	void reset();
	void start(int total);

protected:
    void paintBackground(CDC& dc);
    void paintStatus(CDC& dc);
    COLORREF getStatusColor();
    void paintStep(int startX, int endX);

    CWnd* _baseWindow;
    CRect _bounds;

    bool _error;
    int _total;
    int _progress;
    int _progressX;
};


// Construct a ProgressBar
inline ProgressBar::ProgressBar(CWnd* baseWindow, CRect& bounds): 
	_baseWindow(baseWindow), 
	_bounds(bounds), 
	_error(false),
	_total(0), 
	_progress(0), 
	_progressX(0)
{
    WINDOWINFO wi;
    wi.cbSize = sizeof(WINDOWINFO);
    baseWindow->GetWindowInfo(&wi);
    _bounds.OffsetRect(-wi.rcClient.left, -wi.rcClient.top);
}


// Get the current color
inline COLORREF ProgressBar::getStatusColor()
{
	return _error ? RGB(255, 0, 0) : RGB(0, 255, 0);
}


} // namespace CppUnit


#endif // ProgressBar_INCLUDED
