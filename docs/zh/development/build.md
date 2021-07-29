# 如何构建 ClickHouse 发布包 {#ru-he-gou-jian-clickhouse-fa-bu-bao}

## 安装 Git 和 Pbuilder {#an-zhuang-git-he-pbuilder}

``` bash
sudo apt-get update
sudo apt-get install git pbuilder debhelper lsb-release fakeroot sudo debian-archive-keyring debian-keyring
```

## 拉取 ClickHouse 源码 {#la-qu-clickhouse-yuan-ma}

``` bash
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
cd ClickHouse
```

## 运行发布脚本 {#yun-xing-fa-bu-jiao-ben}

``` bash
./release
```

# 如何在开发过程中编译 ClickHouse {#ru-he-zai-kai-fa-guo-cheng-zhong-bian-yi-clickhouse}

以下教程是在 Ubuntu Linux 中进行编译的示例。
通过适当的更改，它应该可以适用于任何其他的 Linux 发行版。
仅支持具有 x86_64、AArch64。 对 Power9 的支持是实验性的。

## 安装 Git 和 CMake 和 Ninja {#an-zhuang-git-he-cmake-he-ninja}

``` bash
sudo apt-get install git cmake ninja-build
```

或cmake3而不是旧系统上的cmake。
或者在早期版本的系统中用 cmake3 替代 cmake

## 安装 Clang

On Ubuntu/Debian you can use the automatic installation script (check [official webpage](https://apt.llvm.org/))

```bash
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
```

## 拉取 ClickHouse 源码 {#la-qu-clickhouse-yuan-ma-1}

``` bash
git clone --recursive git@github.com:ClickHouse/ClickHouse.git
# or: git clone --recursive https://github.com/ClickHouse/ClickHouse.git

cd ClickHouse
```

## 编译 ClickHouse {#bian-yi-clickhouse}

``` bash
mkdir build
cd build
cmake ..
ninja
cd ..
```

若要创建一个执行文件， 执行 `ninja clickhouse`。
这个命令会使得 `programs/clickhouse` 文件可执行，您可以使用 `client` 或 `server` 参数运行。

[来源文章](https://clickhouse.tech/docs/en/development/build/) <!--hide-->
