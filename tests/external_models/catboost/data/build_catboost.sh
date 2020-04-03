#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $DIR
git clone https://github.com/catboost/catboost.git


cd "${DIR}/catboost/catboost/libs/model_interface"
../../../ya make -r  -o "${DIR}/build/lib" -j4
cd $DIR
ln -sf "${DIR}/build/lib/catboost/libs/model_interface/libcatboostmodel.so" libcatboostmodel.so

cd "${DIR}/catboost/catboost/python-package/catboost"
../../../ya make -r -DUSE_ARCADIA_PYTHON=no -DOS_SDK=local -DPYTHON_CONFIG=python2-config -j4
cd $DIR
ln -sf "${DIR}/catboost/catboost/python-package" python-package
