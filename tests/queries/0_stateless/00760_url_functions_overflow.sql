SELECT extractURLParameter('?_', '\0_________________________________');
SELECT extractURLParameter('?abc=def', 'abc\0def');
SELECT extractURLParameter('?abc\0def=Hello', 'abc\0def');
SELECT extractURLParameter('?_', '\0');
SELECT extractURLParameter('ZiqSZeh?', '\0');
SELECT 'Xx|sfF', match('', '\0'), [], ( SELECT cutURLParameter('C,Ai?X', '\0') ), '\0';
