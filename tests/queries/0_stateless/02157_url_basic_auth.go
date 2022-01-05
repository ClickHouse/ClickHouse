package main

import (
	"net/http"
	"github.com/gin-gonic/gin"
)

func main() {
	accounts := gin.Accounts{
		"admin1": "password",
		"admin2": "password/",
		"admin3?/": "PassWord^#?/",
		"admin4*%": "ok",
		}
	router := gin.New()
	router.Use(gin.BasicAuth(accounts))
	router.GET("/example", func(c *gin.Context) {
		c.String(http.StatusOK, c.MustGet(gin.AuthUserKey).(string))
	})
	router.Run("127.0.0.1:33339")
	//router.RunTLS("example.com:80", "./tls.crt", "tls.key")
}