# Synchronizes submodules' remote URL with .gitmodules
git submodule sync --recursive
# Update the registered submodules with initialize not yet initialized
git submodule update --init --recursive
# Reset all changes done after HEAD
git submodule foreach git reset --hard
# Clean files from .gitignore
git submodule foreach git clean -xfd
# Repeat last 4 commands for all submodule
git submodule foreach git submodule sync --recursive
git submodule foreach git submodule update --init --recursive
git submodule foreach git submodule foreach git reset --hard
git submodule foreach git submodule foreach git clean -xfd
