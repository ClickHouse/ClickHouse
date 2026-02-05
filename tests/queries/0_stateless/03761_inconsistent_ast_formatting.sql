set enable_analyzer=1;

explain syntax select (-(42))[3];
explain syntax select(-('a')).1;
