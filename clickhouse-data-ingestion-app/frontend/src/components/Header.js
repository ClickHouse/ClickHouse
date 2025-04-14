import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import AppBar from '@mui/material/AppBar';
import Box from '@mui/material/Box';
import Toolbar from '@mui/material/Toolbar';
import Typography from '@mui/material/Typography';
import Button from '@mui/material/Button';
import StorageIcon from '@mui/icons-material/Storage';
import Tabs from '@mui/material/Tabs';
import Tab from '@mui/material/Tab';

const Header = () => {
  const location = useLocation();
  const path = location.pathname;

  const routes = [
    { path: '/', label: 'Home' },
    { path: '/clickhouse-to-file', label: 'ClickHouse → File' },
    { path: '/file-to-clickhouse', label: 'File → ClickHouse' },
  ];

  const value = routes.findIndex((route) => route.path === path);

  return (
    <Box sx={{ flexGrow: 1 }}>
      <AppBar position="static">
        <Toolbar>
          <StorageIcon sx={{ mr: 1 }} />
          <Typography
            variant="h6"
            component={Link}
            to="/"
            sx={{
              mr: 2,
              fontWeight: 700,
              color: 'white',
              textDecoration: 'none',
            }}
          >
            ClickHouse Data Ingestion
          </Typography>
          <Tabs
            value={value !== -1 ? value : false}
            textColor="inherit"
            indicatorColor="secondary"
            sx={{ flexGrow: 1 }}
          >
            {routes.map((route, index) => (
              <Tab
                key={route.path}
                label={route.label}
                component={Link}
                to={route.path}
                sx={{ color: 'white' }}
              />
            ))}
          </Tabs>
          <Button
            color="inherit"
            href="https://clickhouse.com/docs"
            target="_blank"
            rel="noopener noreferrer"
          >
            Docs
          </Button>
        </Toolbar>
      </AppBar>
    </Box>
  );
};

export default Header; 