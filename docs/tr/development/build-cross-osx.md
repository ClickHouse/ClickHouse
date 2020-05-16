---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 66
toc_title: "Mac OS X i\xE7in Linux'ta ClickHouse nas\u0131l olu\u015Fturulur"
---

# Mac OS X için Linux'ta ClickHouse nasıl oluşturulur {#how-to-build-clickhouse-on-linux-for-mac-os-x}

Bu, Linux makineniz olduğunda ve onu oluşturmak için kullanmak istediğinizde geçerlidir `clickhouse` OS X üzerinde çalışacak ikili. bu, Linux sunucularında çalışan sürekli entegrasyon kontrolleri için tasarlanmıştır. Clickhouse'u doğrudan Mac OS X'te oluşturmak istiyorsanız, devam edin [başka bir talimat](build-osx.md).

Mac OS X için çapraz yapı, [Inşa talimatları](build.md) önce onları takip et.

# Clang-8'i Yükle {#install-clang-8}

Yönergeleri izleyin https://apt.llvm.org / Ubuntu veya Debian kurulumunuz için.
Örneğin biyonik için komutlar gibidir:

``` bash
sudo echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" >> /etc/apt/sources.list
sudo apt-get install clang-8
```

# Çapraz Derleme Araç Setini Yükle {#install-cross-compilation-toolset}

Yüklediğimiz yolu hatırlayalım `cctools` olarak $ {CCTOOLS}

``` bash
mkdir ${CCTOOLS}

git clone https://github.com/tpoechtrager/apple-libtapi.git
cd apple-libtapi
INSTALLPREFIX=${CCTOOLS} ./build.sh
./install.sh
cd ..

git clone https://github.com/tpoechtrager/cctools-port.git
cd cctools-port/cctools
./configure --prefix=${CCTOOLS} --with-libtapi=${CCTOOLS} --target=x86_64-apple-darwin
make install
```

Ayrıca, MacOS X SDK'YI çalışma ağacına indirmemiz gerekiyor.

``` bash
cd ClickHouse
wget 'https://github.com/phracker/MacOSX-SDKs/releases/download/10.14-beta4/MacOSX10.14.sdk.tar.xz'
mkdir -p build-darwin/cmake/toolchain/darwin-x86_64
tar xJf MacOSX10.14.sdk.tar.xz -C build-darwin/cmake/toolchain/darwin-x86_64 --strip-components=1
```

# ClickHouse İnşa {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-osx
CC=clang-8 CXX=clang++-8 cmake . -Bbuild-osx -DCMAKE_TOOLCHAIN_FILE=cmake/darwin/toolchain-x86_64.cmake \
    -DCMAKE_AR:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ar \
    -DCMAKE_RANLIB:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ranlib \
    -DLINKER_NAME=${CCTOOLS}/bin/x86_64-apple-darwin-ld
ninja -C build-osx
```

Ortaya çıkan ikili bir Mach-O yürütülebilir biçimine sahip olacak ve Linux üzerinde çalıştırılamaz.
