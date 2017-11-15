Functions for working with URLs
-------------------------------

All these functions don't follow the RFC. They are maximally simplified for improved performance.

Functions tat extract part of the URL
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If there isn't anything similar in a URL, an empty string is returned.

protocol
""""""""
Selects the protocol. Examples: http, ftp, mailto, magnet...

domain
""""""
Selects the domain.

domainWithoutWWW
""""""""""""""""
Selects the domain and removes no more than one 'www.' from the beginning of it, if present.

topLevelDomain
""""""""""""""
Selects the top-level domain. Example: .ru.

firstSignificantSubdomain
"""""""""""""""""""""""""
Selects the "first significant subdomain". This is a non-standard concept specific to Yandex.Metrica. The first significant subdomain is a second-level domain if it is 'com', 'net', 'org', or 'co'. Otherwise, it is a third-level domain. For example, firstSignificantSubdomain('https://news.yandex.ru/') = 'yandex', firstSignificantSubdomain('https://news.yandex.com.tr/') = 'yandex'. The list of "insignificant" second-level domains and other implementation details may change in the future.

cutToFirstSignificantSubdomain
""""""""""""""""""""""""""""""
Selects the part of the domain that includes top-level subdomains up to the "first significant subdomain" (see the explanation above).

For example, ``cutToFirstSignificantSubdomain('https://news.yandex.com.tr/') = 'yandex.com.tr'``.

path
""""
Selects the path. Example: /top/news.html The path does not include the query-string.

pathFull
""""""""
The same as above, but including query-string and fragment. Example: /top/news.html?page=2#comments

queryString
"""""""""""
Selects the query-string. Example: page=1&lr=213. query-string does not include the first question mark, or # and everything that comes after #.

fragment
""""""""
Selects the fragment identifier. fragment does not include the first number sign (#).

queryStringAndFragment
""""""""""""""""""""""
Selects the query-string and fragment identifier. Example: page=1#29390.

extractURLParameter(URL, name)
""""""""""""""""""""""""""""""
Selects the value of the 'name' parameter in the URL, if present. Otherwise, selects an empty string. If there are many parameters with this name, it returns the first occurrence. This function works under the assumption that the parameter name is encoded in the URL in exactly the same way as in the argument passed.

extractURLParameters(URL)
"""""""""""""""""""""""""
Gets an array of name=value strings corresponding to the URL parameters. The values are not decoded in any way.

extractURLParameterNames(URL)
"""""""""""""""""""""""""""""
Gets an array of name=value strings corresponding to the names of URL parameters. The values are not decoded in any way.

URLHierarchy(URL)
"""""""""""""""""
Gets an array containing the URL trimmed to the ``/``, ``?`` characters in the path and query-string. Consecutive separator characters are counted as one. The cut is made in the position after all the consecutive separator characters. Example:

URLPathHierarchy(URL)
"""""""""""""""""""""
The same thing, but without the protocol and host in the result. The / element (root) is not included. Example:
This function is used for implementing tree-view reports by URL in Yandex.Metrica.

.. code-block:: text

  URLPathHierarchy('https://example.com/browse/CONV-6788') =
  [
      '/browse/',
      '/browse/CONV-6788'
  ]

decodeURLComponent(URL)
"""""""""""""""""""""""
Returns a URL-decoded URL.

Example:

.. code-block:: sql

  SELECT decodeURLComponent('http://127.0.0.1:8123/?query=SELECT%201%3B') AS DecodedURL;
  
.. code-block:: text

  ┌─DecodedURL─────────────────────────────┐
  │ http://127.0.0.1:8123/?query=SELECT 1; │
  └────────────────────────────────────────┘
  
Functions that remove part of a URL.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If the URL doesn't have anything similar, the URL remains unchanged.

cutWWW
""""""
Removes no more than one 'www.' from the beginning of the URL's domain, if present.

cutQueryString
""""""""""""""
Removes the query-string. The question mark is also removed..

cutFragment
"""""""""""
Removes the fragment identifier. The number sign is also removed.

cutQueryStringAndFragment
"""""""""""""""""""""""""
Removes the query-string and fragment identifier. The question mark and number sign are also removed.

cutURLParameter(URL, name)
""""""""""""""""""""""""""
Removes the URL parameter named 'name', if present. This function works under the assumption that the parameter name is encoded in the URL exactly the same way as in the passed argument.
