SET allow_experimental_window_view = 1;
SELECT hopEnd(tuple([[[(20,20),(50,20),(50,50),(20,50)],[(30,30),(50,50),(50,30)]], [[(20,20),(50,20),(50,50),(20,50)],[(30,30),(50,50),(50,30)]], [[(20,20),(50,20),(50,50),(20,50)],[(30,30),(50,50),(50,30)]]])::Tuple(MultiPolygon)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
