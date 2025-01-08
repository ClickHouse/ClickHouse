from praktika._settings import _Settings
from praktika.mangle import _get_user_settings

Settings = _Settings()

user_settings = _get_user_settings()
for setting, value in user_settings.items():
    Settings.__setattr__(setting, value)
