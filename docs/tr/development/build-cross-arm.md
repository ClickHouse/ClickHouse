---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 67
toc_title: "AARCH64 (ARM64) i\xE7in Linux'ta ClickHouse nas\u0131l olu\u015Fturulur)"
---

# AARCH64 (ARM64) mimarisi için Linux'ta ClickHouse nasıl oluşturulur {#how-to-build-clickhouse-on-linux-for-aarch64-arm64-architecture}

Bu, Linux makineniz olduğunda ve onu oluşturmak için kullanmak istediğinizde geçerlidir `clickhouse` AARCH64 CPU mimarisi ile başka bir Linux makinede çalışacak ikili. Bu, Linux sunucularında çalışan sürekli entegrasyon kontrolleri için tasarlanmıştır.

AARCH64 için çapraz yapı, [Inşa talimatları](build.md) önce onları takip et.

# Clang-8'i Yükle {#install-clang-8}

Yönergeleri izleyin https://apt.llvm.org / Ubuntu veya Debian kurulumunuz için.
Örneğin, Ubuntu Bionic'te aşağıdaki komutları kullanabilirsiniz:

``` bash
echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" | sudo tee /etc/apt/sources.list.d/llvm.list
sudo apt-get update
sudo apt-get install clang-8
```

# Çapraz Derleme Araç Setini Yükle {#install-cross-compilation-toolset}

``` bash
cd ClickHouse
mkdir -p build-aarch64/cmake/toolchain/linux-aarch64
wget 'https://developer.arm.com/-/media/Files/downloads/gnu-a/8.3-2019.03/binrel/gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz?revision=2e88a73f-d233-4f96-b1f4-d8b36e9bb0b9&la=en' -O gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz
tar xJf gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz -C build-aarch64/cmake/toolchain/linux-aarch64 --strip-components=1
```

# ClickHouse İnşa {#build-clickhouse}

``` bash
cd ClickHouse
mkdir build-arm64
CC=clang-8 CXX=clang++-8 cmake . -Bbuild-arm64 -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake
ninja -C build-arm64
```

Ortaya çıkan ikili, yalnızca AARCH64 CPU mimarisi ile Linux'ta çalışacaktır.
