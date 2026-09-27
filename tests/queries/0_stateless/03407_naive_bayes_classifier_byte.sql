-- Tags: no-fasttest
-- no-fasttest: depends on model binary and model details via config files

/*
Output language code mapping:
  Bengali           0
  Mandarin Chinese  1
  German            2
  Greek             3
  English           4
  French            5
  Russian           6
  Spanish           7
*/

SELECT naiveBayesClassifier('lang_byte_2', 'বইটি টেবিলের উপর রাখা আছে।');
SELECT naiveBayesClassifier('lang_byte_2', '他们正在公园里散步');
SELECT naiveBayesClassifier('lang_byte_2', 'Er kocht Suppe für seine Familie');
SELECT naiveBayesClassifier('lang_byte_2', 'Η βροχή σταμάτησε πριν από λίγο');
SELECT naiveBayesClassifier('lang_byte_2', 'She painted the wall a bright yellow');
SELECT naiveBayesClassifier('lang_byte_2', 'Nous attendons le bus depuis dix minutes.');
SELECT naiveBayesClassifier('lang_byte_2', 'На кухне пахнет свежим хлебом.');
SELECT naiveBayesClassifier('lang_byte_2', 'Los niños juegan en la arena.');
