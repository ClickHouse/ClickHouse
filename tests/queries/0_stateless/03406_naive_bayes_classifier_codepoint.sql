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

SELECT naiveBayesClassifier('lang_codepoint_1', 'আজ আকাশটা খুব পরিষ্কার।');
SELECT naiveBayesClassifier('lang_codepoint_1', '她每天早上喝一杯绿茶');
SELECT naiveBayesClassifier('lang_codepoint_1', 'Der Hund schläft unter dem Tisch.');
SELECT naiveBayesClassifier('lang_codepoint_1', 'Το ποδήλατο είναι δίπλα στο δέντρο.');
SELECT naiveBayesClassifier('lang_codepoint_1', 'He forgot his umbrella at the cafe.');
SELECT naiveBayesClassifier('lang_codepoint_1', 'Le chat regarde par la fenêtre');
SELECT naiveBayesClassifier('lang_codepoint_1', 'Мы гуляли в парке до заката');
SELECT naiveBayesClassifier('lang_codepoint_1', 'Ella escribe una carta a su abuela');
