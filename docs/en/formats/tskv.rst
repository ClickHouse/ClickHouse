TSKV
----

Similar to TabSeparated, but displays data in name=value format. Names are displayed just as in TabSeparated. Additionally, a ``=`` symbol is displayed.

.. code-block:: text

  SearchPhrase=   count()=8267016
  SearchPhrase=bathroom interior    count()=2166
  SearchPhrase=yandex     count()=1655
  SearchPhrase=spring 2014 fashion    count()=1549
  SearchPhrase=free-form photo       count()=1480
  SearchPhrase=Angelina Jolie    count()=1245
  SearchPhrase=omsk       count()=1112
  SearchPhrase=photos of dog breeds    count()=1091
  SearchPhrase=curtain design        count()=1064
  SearchPhrase=baku       count()=1000

In case of many small columns this format is obviously not effective and there usually is no reason to use it. This format is supported because it is used for some cases in Yandex.

Format is supported both for input and output. In INSERT queries data can be supplied with arbitrary order of columns. It is also possible to omit values in which case the default value of the column is inserted. N.B. when using TSKV format, complex default values are not supported, so when omitting a column its value will be zeros or empty string depending on its type.
