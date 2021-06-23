# Finds all .gcno files in CH build directories and copies them to a special directory passed as first arg.
# Launched in Yandex.Sandbox, docker container has a mounted volume with all .gcno files.
# Motivation: build directory is not copied to docker so we need to get .gcno files beforehand.
find . -type f -name "*.gcno" -exec cp {} $1 \;
