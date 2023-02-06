# POCO_STATIC=1 - for static build
# POCO_UNBUNDLED - for no built-in version of libs
# CMAKE_INSTALL_PREFIX=path - for install path

mkdir cmake-build
cd cmake-build

cmake ../. -DCMAKE_BUILD_TYPE=Debug -G"NMake Makefiles JOM" %1 %2 %3 %4 %5
jom /j3
jom install

del CMakeCache.txt

cmake ../. -DCMAKE_BUILD_TYPE=Release -G"NMake Makefiles JOM" %1 %2 %3 %4 %5
jom /j3
jom install


cd ..
