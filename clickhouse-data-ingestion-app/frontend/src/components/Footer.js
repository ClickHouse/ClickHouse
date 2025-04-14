import React from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Link from '@mui/material/Link';

const Footer = () => {
  return (
    <Box
      component="footer"
      sx={{
        py: 3,
        px: 2,
        mt: 'auto',
        backgroundColor: (theme) => theme.palette.grey[100],
        borderTop: '1px solid',
        borderColor: 'divider',
      }}
    >
      <Typography variant="body2" color="text.secondary" align="center">
        {'© '}
        {new Date().getFullYear()}{' '}
        <Link color="inherit" href="https://clickhouse.com/" target="_blank" rel="noopener">
          ClickHouse
        </Link>{' '}
        Data Ingestion Tool
      </Typography>
    </Box>
  );
};

export default Footer; 