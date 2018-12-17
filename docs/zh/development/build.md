# 如何构建 ClickHouse 发布包

## 安装 Git 和 Pbuilder

```bash
sudo apt-get update
sudo apt-get install git pbuilder debhelper lsb-release fakeroot sudo debian-archive-keyring debian-keyring
```

## 拉取 ClickHouse 源码

```bash
git clone --recursive --branch stable https://github.com/yandex/ClickHouse.git
cd ClickHouse
```

## 运行发布脚本

```bash
./release
```

# 如何在开发过程中编译 ClickHouse

以下教程是在 Ubuntu Linux 中进行编译的示例。
通过适当的更改，它应该可以适用于任何其他的 Linux 发行版。
仅支持具有 SSE 4.2的 x86_64。 对 AArch64 的支持是实验性的。

测试是否支持 SSE 4.2，执行：

```bash
grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

## 安装 Git 和 CMake

```bash
sudo apt-get install git cmake ninja-build
```

Or cmake3 instead of cmake on older systems.
或者在早期版本的系统中用 cmake3 替代 cmake

## 安装 GCC 7

There are several ways to do this.

### 安装 PPA 包

```bash
sudo apt-get install software-properties-common
sudo apt-add-repository ppa:ubuntu-toolchain-r/test
sudo apt-get update
sudo apt-get install gcc-7 g++-7
```

### 源码安装 gcc

请查看 [ci/build-gcc-from-sources.sh](https://github.com/yandex/ClickHouse/blob/master/ci/build-gcc-from-sources.sh)

## 使用 GCC 7 来编译

```bash
export CC=gcc-7
export CXX=g++-7
```

## 安装所需的工具依赖库

```bash
sudo apt-get install libicu-dev libreadline-dev
```

## 拉取 ClickHouse 源码

```bash
git clone --recursive git@github.com:yandex/ClickHouse.git
# or: git clone --recursive https://github.com/yandex/ClickHouse.git

cd ClickHouse
```

For the latest stable version, switch to the `stable` branch.

## 编译 ClickHouse

```bash
mkdir build
cd build
cmake ..
ninja
cd ..
```

若要创建一个执行文件， 执行 `ninja clickhouse`。
这个命令会使得 `dbms/programs/clickhouse` 文件可执行，您可以使用 `client` or `server` 参数运行。


[来源文章](https://clickhouse.yandex/docs/en/development/build/) <!--hide-->
