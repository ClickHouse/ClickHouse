cpplint - static code checker for C++
=====================================

.. image:: https://travis-ci.org/cpplint/cpplint.svg?branch=master
    :target: https://travis-ci.org/cpplint/cpplint

.. image:: https://img.shields.io/pypi/v/cpplint.svg
    :target: https://pypi.python.org/pypi/cpplint

.. image:: https://img.shields.io/pypi/pyversions/cpplint.svg
    :target: https://pypi.python.org/pypi/cpplint

.. image:: https://img.shields.io/pypi/status/cpplint.svg
    :target: https://pypi.python.org/pypi/cpplint

.. image:: https://img.shields.io/pypi/l/cpplint.svg
    :target: https://pypi.python.org/pypi/cpplint

.. image:: https://img.shields.io/pypi/dd/cpplint.svg
    :target: https://pypi.python.org/pypi/cpplint

.. image:: https://img.shields.io/pypi/dw/cpplint.svg
    :target: https://pypi.python.org/pypi/cpplint

.. image:: https://img.shields.io/pypi/dm/cpplint.svg
    :target: https://pypi.python.org/pypi/cpplint

Cpplint is a command-line tool to check C/C++ files for style issues following `Google's C++ style guide <http://google.github.io/styleguide/cppguide.html>`_.
Cpplint is developed and maintained by Google Inc. at `google/styleguide <https://github.com/google/styleguide>`_, also see see the `wikipedia entry <http://en.wikipedia.org/wiki/Cpplint>`_

While Google maintains cpplint, Google is not (very) responsive to issues and pull requests, this fork aims to be (somewhat) more open to add fixes to cpplint to enable fixes, when those fixes make cpplint usable in wider contexts.


Installation
============


To install cpplint from PyPI, run:

.. code-block:: bash

    $ pip install cpplint

Then run it with:

.. code-block:: bash

    $ cpplint [OPTIONS] files

For full usage instructions, run:

.. code-block:: bash

    $ cpplint --help

Changes
-------

The modifications in this fork are minor fixes and cosmetic changes:

* more default extensions
* python 3 compatibility
* customizable file extensions with the --extensions argument
* continuous integration on travis
* support for recursive file discovery via the --recursive argument
* support for excluding files via --exclude
* JUnit XML output format
* Overriding repository root auto-detection via --repository
* Support ``#pragma once`` as an alternative to header include guards


Acknowledgements
----------------

Thanks to Google Inc. for open-sourcing their in-house tool.
Thanks to maintainers of the fork

* `tkruse <https://github.com/tkruse>`_  
* `mattyclarkson <https://github.com/mattyclarkson>`_
* `theandrewdavis <https://github.com/theandrewdavis>`_
